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
package org.apache.apex.malhar.lib.wal;

import java.io.IOException;

import com.datatorrent.lib.fileaccess.FileAccess;

public class RollingFSWal<T> implements WAL<T, RollingFSWal.WalPointer>
{
  private final String WAL_FILE_PREFIX = "wal";
  private final FileAccess bfs;
  private final Serde<T> serializer;
  private final long bucketKey;
  private String baseDir;

  public RollingFSWal(FileAccess bfs, WAL.Serde<T> serializer, long bucketKey)
  {
    this.bfs = bfs;
    this.serializer = serializer;
    this.bucketKey = bucketKey;
  }

  @Override
  public WALReader<T, WalPointer> getReader() throws IOException
  {
    return new RollingFSWalReader();
  }

  @Override
  public WALWriter<T, WalPointer> getWriter() throws IOException
  {
    return new RollingFSWALWriter();
  }

  protected String getName(int id)
  {
    return baseDir + "/" + WAL_FILE_PREFIX + "_" + id;
  }

  public String getBaseDir()
  {
    return baseDir;
  }

  public void setBaseDir(String baseDir)
  {
    this.baseDir = baseDir;
  }

  public static class WalPointer implements Comparable<WalPointer>
  {
    public int fileId;
    public long offset;

    public WalPointer(int fileId, long offset)
    {
      this.fileId = fileId;
      this.offset = offset;
    }

    @Override
    public int compareTo(WalPointer o)
    {
      if (this.fileId < o.fileId) {
        return -1;
      }
      if (this.fileId > o.fileId) {
        return 1;
      }
      if (this.offset < o.offset) {
        return -1;
      }
      if (this.offset > o.offset) {
        return 1;
      }
      return 0;
    }

    @Override
    public String toString()
    {
      return "WalPointer{" +
        "fileId=" + fileId +
        ", offset=" + offset +
        '}';
    }
  }

  public class RollingFSWalReader implements WAL.WALReader<T, WalPointer>
  {
    private WALReader<T, Long> currentReader = null;
    private WalPointer currOffset;
    private T entry;
    private boolean eof = false;

    public RollingFSWalReader() throws IOException
    {
      this.currOffset = new WalPointer(0, 0);
    }

    @Override
    public WalPointer getOffset()
    {
      return currOffset;
    }

    @Override
    public void close() throws IOException
    {
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
    }

    @Override
    public void seek(WalPointer offset) throws IOException
    {
      currentReader = new FSWal<T>(bfs, serializer, bucketKey, getName(offset.fileId)).getReader();
      currentReader.seek(offset.offset);
      currOffset = offset;
      eof = false;
    }

    /**
     * Move to the next WAL segment.
     *
     * @return
     * @throws IOException
     */
    private boolean nextSegment() throws IOException
    {
      if (currentReader == null) {
        int fileId = currOffset.fileId;
        fileId++;
        if (bfs.exists(bucketKey, getName(fileId))) {
          currentReader = new FSWal<T>(bfs, serializer, bucketKey, getName(fileId)).getReader();
          currOffset = new WalPointer(fileId, 0);
          return true;
        } else {
          eof = true;
          entry = null;
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean advance() throws IOException
    {
      if (eof) {
        return false;
      }

      boolean ret = false;
      do {
        if (!nextSegment()) {
          break;
        }

        ret = currentReader.advance();
        if (ret) {
          entry = currentReader.get();
          break;
        } else {
          close();
          currentReader = null;
        }

      } while (true);

      return ret;
    }

    @Override
    public T get()
    {
      return entry;
    }
  }

  public class RollingFSWALWriter implements WAL.WALWriter<T, WalPointer>
  {
    private transient WALWriter<T, Long> currentWriter = null;
    private int walFileId = -1;
    private long bucketKey = 0;
    private long maxSegmentSize = 128 * 1024 * 1024; // 128MB default.

    @Override
    public void close() throws IOException
    {
      if (currentWriter != null) {
        currentWriter.close();
      }
    }

    @Override
    public int append(T entry) throws IOException
    {
      logMayRoll();
      if (currentWriter == null) {
        currentWriter = new FSWal(bfs, serializer, bucketKey, getName(walFileId)).getWriter();
      }

      int len = currentWriter.append(entry);
      return len;
    }

    @Override
    public void flush() throws IOException
    {
      if (currentWriter != null) {
        currentWriter.flush();
      }
    }

    @Override
    public WalPointer getOffset()
    {
      return new WalPointer(walFileId, currentWriter.getOffset());
    }

    void logMayRoll() throws IOException
    {
      if (currentWriter == null) {
        newWalSegment();
      } else {
        if (currentWriter.getOffset() >= maxSegmentSize) {
          currentWriter.close();
          newWalSegment();
        }
      }
    }

    private void newWalSegment() throws IOException
    {
      walFileId++;
      currentWriter = new FSWal(bfs, serializer, bucketKey, getName(walFileId)).getWriter();
    }

    public long getMaxSegmentSize()
    {
      return maxSegmentSize;
    }

    public void setMaxSegmentSize(long maxSegmentSize)
    {
      this.maxSegmentSize = maxSegmentSize;
    }
  }
}
