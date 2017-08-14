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

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

/**
 * Represents the metadata of a block.
 *
 * @since 2.1.0
 */
public interface BlockMetadata
{
  /**
   * @return unique block id.
   */
  long getBlockId();

  /**
   * @return the start offset associated with the block.
   */
  long getOffset();

  /**
   * @return the length of the source in the block.
   */
  long getLength();

  /**
   * @return if this is the last block in file.
   */
  boolean isLastBlock();

  /**
   * @return the previous block id.
   */
  long getPreviousBlockId();

  abstract class AbstractBlockMetadata implements BlockMetadata
  {
    private long offset;
    private long length;
    private boolean isLastBlock;
    private long previousBlockId;
    private long blockId;

    @SuppressWarnings("unused")
    protected AbstractBlockMetadata()
    {
      offset = -1;
      length = -1;
      isLastBlock = false;
      previousBlockId = -1;
      blockId = -1;
    }

    /**
     * Constructs Block metadata
     *
     * @param offset          offset of the file in the block
     * @param length          length of the file in the block
     * @param isLastBlock     true if this is the last block of file
     * @param previousBlockId id of the previous block
     */
    @Deprecated
    public AbstractBlockMetadata(long offset, long length, boolean isLastBlock, long previousBlockId)
    {
      this.offset = offset;
      this.length = length;
      this.isLastBlock = isLastBlock;
      this.previousBlockId = previousBlockId;
      this.blockId = -1;
    }

    /**
     * Constructs Block metadata
     *
     * @param blockId         block id
     * @param offset          offset of the file in the block
     * @param length          length of the file in the block
     * @param isLastBlock     true if this is the last block of file
     * @param previousBlockId id of the previous block
     */
    public AbstractBlockMetadata(long blockId, long offset, long length, boolean isLastBlock, long previousBlockId)
    {
      this.blockId = blockId;
      this.offset = offset;
      this.length = length;
      this.isLastBlock = isLastBlock;
      this.previousBlockId = previousBlockId;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AbstractBlockMetadata)) {
        return false;
      }

      AbstractBlockMetadata that = (AbstractBlockMetadata)o;
      return getBlockId() == that.getBlockId();
    }

    @Override
    public int hashCode()
    {
      return (int)getBlockId();
    }

    @Override
    public long getOffset()
    {
      return offset;
    }

    /**
     * Sets the offset of the file in the block.
     */
    public void setOffset(long offset)
    {
      this.offset = offset;
    }

    @Override
    public long getLength()
    {
      return length;
    }

    /**
     * Sets the length of the file in the block.
     */
    public void setLength(long length)
    {
      this.length = length;
    }

    @Override
    public boolean isLastBlock()
    {
      return isLastBlock;
    }

    public void setLastBlock(boolean lastBlock)
    {
      this.isLastBlock = lastBlock;
    }

    @Override
    public long getPreviousBlockId()
    {
      return previousBlockId;
    }

    /**
     * Sets the previous block id.
     *
     * @param previousBlockId previous block id.
     */
    public void setPreviousBlockId(long previousBlockId)
    {
      this.previousBlockId = previousBlockId;
    }

    @Override
    public long getBlockId()
    {
      return blockId;
    }

    public void setBlockId(long blockId)
    {
      this.blockId = blockId;
    }
  }

  /**
   * A block of file which contains file path and other block properties.
   * It also controls if blocks should be read in sequence
   */
  class FileBlockMetadata extends AbstractBlockMetadata
  {
    private final String filePath;
    private long fileLength;

    protected FileBlockMetadata()
    {
      super();
      filePath = null;
    }

    public FileBlockMetadata(String filePath, long blockId, long offset, long length, boolean isLastBlock,
        long previousBlockId)
    {
      super(blockId, offset, length, isLastBlock, previousBlockId);
      this.filePath = filePath;
    }

    public FileBlockMetadata(String filePath, long blockId, long offset, long length, boolean isLastBlock,
        long previousBlockId, long fileLength)
    {
      super(blockId, offset, length, isLastBlock, previousBlockId);
      this.filePath = filePath;
      this.fileLength = fileLength;
    }

    public FileBlockMetadata(String filePath)
    {
      this.filePath = filePath;
    }

    public FileBlockMetadata(String filePath, long fileLength)
    {
      this.filePath = filePath;
      this.fileLength = fileLength;
    }

    public String getFilePath()
    {
      return filePath;
    }

    /**
     * Returns the length of the file to which this block belongs
     *
     * @return length of the file to which this block belongs
     */
    public long getFileLength()
    {
      return fileLength;
    }

    /**
     * Set the length of the file to which this block belongs
     *
     * @param fileLength
     */
    public void setFileLength(long fileLength)
    {
      this.fileLength = fileLength;
    }

    public FileBlockMetadata newInstance(@NotNull String filePath)
    {
      Preconditions.checkNotNull(filePath);
      return new FileBlockMetadata(filePath);
    }
  }
}
