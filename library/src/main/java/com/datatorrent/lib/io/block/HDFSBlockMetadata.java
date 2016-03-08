package com.datatorrent.lib.io.block;

/**
 * HDFSBlockMetadata extends {@link BlockMetadata} to provide an option if blocks of a file should be read in-sequence
 * or in-parallel
 */
public class HDFSBlockMetadata extends BlockMetadata.FileBlockMetadata
{
  boolean readBlockInSequence;

  protected HDFSBlockMetadata()
  {
    super();
  }

  public HDFSBlockMetadata(String filePath, long blockId, long offset, long length, boolean isLastBlock, long previousBlockId)
  {
    super(filePath, blockId, offset, length, isLastBlock, previousBlockId);
  }

  public HDFSBlockMetadata(String filePath)
  {
    super(filePath);
  }

  @Override
  public int hashCode()
  {
    if (isReadBlockInSequence()) {
      return getFilePath().hashCode();
    }
    return super.hashCode();
  }

  public boolean isReadBlockInSequence()
  {
    return readBlockInSequence;
  }

  public void setReadBlockInSequence(boolean readBlockInSequence)
  {
    this.readBlockInSequence = readBlockInSequence;
  }

}
