/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.block;

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

  public abstract class AbstractBlockMetadata implements BlockMetadata
  {
    private long offset;
    private long length;
    private final boolean isLastBlock;
    private final long previousBlockId;

    @SuppressWarnings("unused")
    protected AbstractBlockMetadata()
    {
      offset = -1;
      length = -1;
      isLastBlock = false;
      previousBlockId = -1;
    }

    /**
     * Constructs Block metadata
     *
     * @param offset          offset of the file in the block
     * @param length          length of the file in the block
     * @param isLastBlock     true if this is the last block of file
     * @param previousBlockId id of the previous block
     */
    public AbstractBlockMetadata(long offset, long length, boolean isLastBlock, long previousBlockId)
    {
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

      AbstractBlockMetadata that = (AbstractBlockMetadata) o;
      return getBlockId() == that.getBlockId();
    }

    @Override
    public int hashCode()
    {
      return (int) getBlockId();
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

    @Override
    public long getPreviousBlockId()
    {
      return previousBlockId;
    }
  }

  /**
   * A block of file which contains file path adn other block properties.
   */
  public static class FileBlockMetadata extends AbstractBlockMetadata
  {
    private final String filePath;
    private final long blockId;

    protected FileBlockMetadata()
    {
      super();
      filePath = null;
      blockId = -1;
    }

    public FileBlockMetadata(String filePath, long blockId, long offset, long length, boolean isLastBlock, long previousBlockId)
    {
      super(offset, length, isLastBlock, previousBlockId);
      this.filePath = filePath;
      this.blockId = blockId;
    }

    @Override
    public long getBlockId()
    {
      return blockId;
    }

    public String getFilePath()
    {
      return filePath;
    }
  }
}
