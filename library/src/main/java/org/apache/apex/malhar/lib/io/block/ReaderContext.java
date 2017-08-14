/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.io.block;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PositionedReadable;

/**
 * This controls how an {@link AbstractBlockReader} reads a {@link BlockMetadata}.
 *
 * @param <STREAM> type of stream
 * @since 2.1.0
 */
public interface ReaderContext<STREAM extends InputStream & PositionedReadable>
{

  /**
   * Initializes the reader context.
   *
   * @param stream           input stream
   * @param blockMetadata    block-metadata
   * @param consecutiveBlock if current block was a consecutive block in the source. if it is then we continue reading
   *                         from the last offset.
   */
  void initialize(STREAM stream, BlockMetadata blockMetadata, boolean consecutiveBlock);

  /**
   * Reads an entity. Returns null when the block is processed.
   *
   * @return {@link Entity}. null when the work is done.
   * @throws IOException
   */
  Entity next() throws IOException;

  /**
   * Represents the total bytes used to construct the record.<br/>
   * Used bytes can be different from the bytes in the record.
   */
  class Entity
  {
    private byte[] record;
    private long usedBytes;

    public void clear()
    {
      record = null;
      usedBytes = -1;
    }

    public byte[] getRecord()
    {
      return record;
    }

    public void setRecord(byte[] record)
    {
      this.record = record;
    }

    public long getUsedBytes()
    {
      return usedBytes;
    }

    public void setUsedBytes(long usedBytes)
    {
      this.usedBytes = usedBytes;
    }
  }

  /**
   * An Abstract reader context which assumes that a block boundary is never crossed if the last entity read is fully
   * contained in the block.
   *
   * @param <STREAM> type of stream.
   */
  abstract class AbstractReaderContext<STREAM extends InputStream & PositionedReadable> implements ReaderContext<STREAM>
  {
    protected transient long offset;
    protected transient STREAM stream;
    protected transient BlockMetadata blockMetadata;

    protected final transient Entity entity;

    protected AbstractReaderContext()
    {
      offset = -1;
      entity = new Entity();
    }

    @Override
    public void initialize(STREAM stream, BlockMetadata blockMetadata, boolean consecutiveBlock)
    {
      this.stream = stream;
      this.blockMetadata = blockMetadata;
      if (!consecutiveBlock) {
        offset = blockMetadata.getOffset();
      }
    }

    @Override
    public Entity next() throws IOException
    {
      if (offset < blockMetadata.getLength()) {
        Entity entity = readEntity();
        offset += entity.usedBytes;
        return entity;
      }
      return null;
    }

    protected abstract Entity readEntity() throws IOException;
  }

  /**
   * This reader context splits the block into entities on '\n' or '\r'.<br/>
   * It will not read ahead of the block boundary if the last entity was completely contained in the block.<br/>
   * Any records formed using this context will need a way to validate the start of the record.
   *
   * @param <STREAM> type of stream.
   */
  class LineReaderContext<STREAM extends InputStream & PositionedReadable> extends AbstractReaderContext<STREAM>
  {

    protected int bufferSize;
    /**
    * overflowBufferSize is the number of bytes fetched when a record overflows
    * to consecutive block
    */
    protected int overflowBufferSize;

    private final transient ByteArrayOutputStream lineBuilder;
    private final transient ByteArrayOutputStream emptyBuilder;
    private final transient ByteArrayOutputStream tmpBuilder;

    protected transient byte[] buffer;
    private transient String bufferStr;
    private transient int posInStr;
    private transient boolean overflowBlockRead;

    public LineReaderContext()
    {
      super();
      bufferSize = 8192;
      overflowBufferSize = 8192;
      lineBuilder = new ByteArrayOutputStream();
      emptyBuilder = new ByteArrayOutputStream();
      tmpBuilder = new ByteArrayOutputStream();
    }

    @Override
    public void initialize(STREAM stream, BlockMetadata blockMetadata, boolean consecutiveBlock)
    {
      overflowBlockRead = false;
      posInStr = 0;
      offset = blockMetadata.getOffset();
      super.initialize(stream, blockMetadata, consecutiveBlock);
    }

    /**
     * Reads bytes from the stream starting from the offset into the buffer
     *
     * @param bytesFromCurrentOffset
     *          bytes read till now from current block
     * @param bytesToFetch
     *          the number of bytes to be read from stream
     * @return the number of bytes actually read, -1 if 0 bytes read
     * @throws IOException
     */
    protected int readData(final long bytesFromCurrentOffset, final int bytesToFetch) throws IOException
    {
      if (buffer == null) {
        buffer = new byte[bytesToFetch];
      }
      return stream.read(offset + bytesFromCurrentOffset, buffer, 0, bytesToFetch);
    }

    /**
     * @param usedBytesFromOffset
     *          number of bytes the pointer is ahead of the offset
     * @return true if end of stream reached, false otherwise
     */
    protected boolean checkEndOfStream(final long usedBytesFromOffset)
    {
      if (!overflowBlockRead) {
        return (offset - blockMetadata.getOffset() + usedBytesFromOffset < bufferSize);
      } else {
        return (offset - blockMetadata.getOffset() + usedBytesFromOffset < overflowBufferSize);
      }
    }

    /**
     * Gives the number of bytes to be fetched from the stream
     *
     * @param overflowBlockRead
     *          indicates whether we are reading main block or overflow block
     * @return bytes to be fetched from stream
     */
    protected int calculateBytesToFetch()
    {
      return (overflowBlockRead ? overflowBufferSize : (bufferSize));
    }

    @Override
    protected Entity readEntity() throws IOException
    {
      //Implemented a buffered reader instead of using java's BufferedReader because it was reading much ahead of block
      // boundary and faced issues with duplicate records. Controlling the buffer size didn't help either.

      boolean foundEOL = false;
      int bytesRead = 0;
      long usedBytes = 0;

      while (!foundEOL) {
        tmpBuilder.reset();
        if (posInStr == 0) {
          int bytesToFetch = calculateBytesToFetch();
          overflowBlockRead = true;
          bytesRead = readData(usedBytes, bytesToFetch);
          if (bytesRead == -1) {
            break;
          }
          bufferStr = new String(buffer,0, bytesRead);
        }

        while (posInStr < bufferStr.length()) {
          char c = bufferStr.charAt(posInStr);
          if (c != '\r' && c != '\n') {
            tmpBuilder.write(c);
            posInStr++;
          } else {
            foundEOL = true;
            break;
          }
        }
        byte[] subLine = tmpBuilder.toByteArray();
        usedBytes += subLine.length;
        lineBuilder.write(subLine);

        if (foundEOL) {
          while (posInStr < bufferStr.length()) {
            char c = bufferStr.charAt(posInStr);
            if (c == '\r' || c == '\n') {
              emptyBuilder.write(c);
              posInStr++;
            } else {
              break;
            }
          }
          usedBytes += emptyBuilder.toByteArray().length;
        } else {
          //end of stream reached
          if (checkEndOfStream(usedBytes)) {
            break;
          }
          //read more bytes from the input stream
          posInStr = 0;
        }
      }
      //when end of stream is reached then bytesRead is -1
      if (bytesRead == -1) {
        lineBuilder.reset();
        emptyBuilder.reset();
        return null;
      }
      entity.clear();
      entity.record = lineBuilder.toByteArray();
      entity.usedBytes = usedBytes;

      lineBuilder.reset();
      emptyBuilder.reset();
      return entity;
    }

    /**
     * Sets the buffer size of read.
     *
     * @param bufferSize size of the buffer
     */
    public void setBufferSize(int bufferSize)
    {
      this.bufferSize = bufferSize;
    }

    /**
     * @return the buffer size of read.
     */
    public int getBufferSize()
    {
      return this.bufferSize;
    }

    /**
     * Sets the overflow buffer size of read.
     *
     * @param overflowBufferSize
     *          size of the overflow buffer
     */
    public void setOverflowBufferSize(int overflowBufferSize)
    {
      this.overflowBufferSize = overflowBufferSize;
    }

    /**
     * @param buffer
     *          the bytes read from the source
     */
    protected void setBuffer(byte[] buffer)
    {
      this.buffer = buffer;
    }

    /**
     * Sets whether to read overflow block during next fetch.
     *
     * @param overflowBlockRead
     *          boolean indicating whether to read overflow block during next read
     */
    public void setOverflowBlockRead(boolean overflowBlockRead)
    {
      this.overflowBlockRead = overflowBlockRead;
    }

    /**
     * Returns a boolean indicating whether to read overflow block during next read
     *
     * @returnoverflowBlockRead
     */
    protected boolean isOverflowBlockRead()
    {
      return overflowBlockRead;
    }
  }

  /**
   * Another reader context that splits the block into records on '\n' or '\r'.<br/>
   * This implementation doesn't need a way to validate the start of a record.<br/>
   * <p/>
   * This  starts parsing the block (except the first block of the file) from the first eol character.
   * It is a less optimized version of an {@link LineReaderContext} which always reads beyond the block
   * boundary.
   *
   * @param <STREAM>
   */
  class ReadAheadLineReaderContext<STREAM extends InputStream & PositionedReadable> extends LineReaderContext<STREAM>
  {
    @Override
    public void initialize(STREAM stream, BlockMetadata blockMetadata, boolean consecutiveBlock)
    {
      super.initialize(stream, blockMetadata, consecutiveBlock);
      //ignore first entity of  all the blocks except the first one because those bytes
      //were used during the parsing of the previous block.
      if (blockMetadata.getPreviousBlockId() != -1 && blockMetadata.getOffset() != 0) {
        try {
          Entity entity = readEntity();
          offset += entity.usedBytes;
        } catch (IOException e) {
          throw new RuntimeException("when reading first entity", e);
        }
      }
    }

    @Override
    public Entity next() throws IOException
    {
      if (offset < blockMetadata.getLength() || (offset == blockMetadata.getLength() && !blockMetadata.isLastBlock())) {
        Entity entity = readEntity();
        offset += entity.usedBytes;
        return entity;
      }
      return null;
    }

    @Override
    protected int calculateBytesToFetch()
    {
      /*
       * With readAheadLineReaderContext, we always read at least one overflowBlock. Hence, fetch it in advance
       */
      return (this.isOverflowBlockRead() ? overflowBufferSize : (bufferSize + overflowBufferSize));
    }
  }

  /**
   * This creates fixed sized entities.<br/>
   * It doesn't read beyond the block boundary therefore the last byte-array could be smaller.<br/>
   *
   * @param <STREAM> type of stream.
   */
  class FixedBytesReaderContext<STREAM extends InputStream & PositionedReadable> extends AbstractReaderContext<STREAM>
  {
    //When this field is null, it is initialized to default fs block size in setup.
    protected Integer length;

    @Override
    public void initialize(STREAM stream, BlockMetadata blockMetadata, boolean consecutiveBlock)
    {
      if (length == null) {
        length = (int)new Configuration().getLong("fs.local.block.size", 32 * 1024 * 1024);
        LOG.debug("length init {}", length);
      }
      super.initialize(stream, blockMetadata, consecutiveBlock);
    }

    @Override
    protected Entity readEntity() throws IOException
    {
      entity.clear();
      int bytesToRead = length;
      if (offset + length >= blockMetadata.getLength()) {
        bytesToRead = (int)(blockMetadata.getLength() - offset);
      }
      byte[] record = new byte[bytesToRead];
      stream.readFully(offset, record, 0, bytesToRead);
      entity.usedBytes = bytesToRead;
      entity.record = record;

      return entity;
    }

    /**
     * Sets the length of each record.
     *
     * @param length fixed length of each record.
     */
    public void setLength(Integer length)
    {
      this.length = length;
    }

    /**
     * @return the length of record.
     */
    public Integer getLength()
    {
      return this.length;
    }

    private static final Logger LOG = LoggerFactory.getLogger(FixedBytesReaderContext.class);
  }
}
