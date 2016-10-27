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
package com.datatorrent.lib.io.fs;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.block.BlockMetadata;

/**
 * An abstract File Splitter.
 *
 * @since 3.2.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractFileSplitter extends BaseOperator
{
  protected Long blockSize;
  private int sequenceNo;

  /**
   * This is a threshold on the no. of blocks emitted per window. A lot of blocks emitted
   * per window can overwhelm the downstream operators. This setting helps to control that.
   */
  @Min(1)
  protected int blocksThreshold;

  protected transient long blockCount;

  protected BlockMetadataIterator blockMetadataIterator;

  protected transient int operatorId;
  protected transient Context.OperatorContext context;
  protected transient long currentWindowId;

  @AutoMetric
  protected int filesProcessed;

  public final transient DefaultOutputPort<FileMetadata> filesMetadataOutput = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<BlockMetadata.FileBlockMetadata> blocksMetadataOutput =
      new DefaultOutputPort<>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    Preconditions.checkArgument(blockSize == null || blockSize > 0, "invalid block size");

    operatorId = context.getId();
    this.context = context;
    currentWindowId = context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID);
    if (blockSize == null) {
      blockSize = getDefaultBlockSize();
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    filesProcessed = 0;
    blockCount = 0;
    currentWindowId = windowId;
  }

  protected void process()
  {
    if (blockMetadataIterator != null && blockCount < blocksThreshold) {
      emitBlockMetadata();
    }

    FileInfo fileInfo;
    while (blockCount < blocksThreshold && (fileInfo = getFileInfo()) != null) {
      if (!processFileInfo(fileInfo)) {
        break;
      }
    }
  }

  /**
   * @return {@link FileInfo}
   */
  protected abstract FileInfo getFileInfo();

  /**
   * @param fileInfo file info
   * @return true if blocks threshold is reached; false otherwise
   */
  protected boolean processFileInfo(FileInfo fileInfo)
  {
    try {
      FileMetadata fileMetadata = buildFileMetadata(fileInfo);
      filesMetadataOutput.emit(fileMetadata);
      filesProcessed++;
      if (!fileMetadata.isDirectory()) {
        blockMetadataIterator = new BlockMetadataIterator(this, fileMetadata, blockSize);
        if (!emitBlockMetadata()) {
          //block threshold reached
          return false;
        }
      }
      return true;
    } catch (IOException e) {
      throw new RuntimeException("creating metadata", e);
    }
  }

  /**
   * @return true if all the blocks were emitted; false otherwise
   */
  protected boolean emitBlockMetadata()
  {
    while (blockMetadataIterator.hasNext()) {
      if (blockCount++ < blocksThreshold) {
        this.blocksMetadataOutput.emit(blockMetadataIterator.next());
      } else {
        return false;
      }
    }
    blockMetadataIterator = null;
    return true;
  }

  /**
   * Builds block metadata
   *
   * @param pos                 offset of the block
   * @param lengthOfFileInBlock length of the block in file
   * @param blockNumber         block number
   * @param fileMetadata        file metadata
   * @param isLast              last block of the file
   * @return block file metadata
   */
  protected BlockMetadata.FileBlockMetadata buildBlockMetadata(long pos, long lengthOfFileInBlock, int blockNumber,
      FileMetadata fileMetadata, boolean isLast)
  {
    BlockMetadata.FileBlockMetadata fileBlockMetadata = createBlockMetadata(fileMetadata);
    fileBlockMetadata.setBlockId(fileMetadata.getBlockIds()[blockNumber - 1]);
    fileBlockMetadata.setOffset(pos);
    fileBlockMetadata.setLength(lengthOfFileInBlock);
    fileBlockMetadata.setLastBlock(isLast);
    fileBlockMetadata.setPreviousBlockId(blockNumber == 1 ? -1 : fileMetadata.getBlockIds()[blockNumber - 2]);

    return fileBlockMetadata;
  }

  /**
   * Can be overridden for creating block metadata of a type that extends {@link BlockMetadata.FileBlockMetadata}
   */
  protected BlockMetadata.FileBlockMetadata createBlockMetadata(FileMetadata fileMetadata)
  {
    return new BlockMetadata.FileBlockMetadata(fileMetadata.getFilePath(), fileMetadata.getFileLength());
  }

  /**
   * Creates file-metadata and populates no. of blocks in the metadata.
   *
   * @param fileInfo file information
   * @return file-metadata
   * @throws IOException
   */
  protected FileMetadata buildFileMetadata(FileInfo fileInfo) throws IOException
  {
    LOG.debug("file {}", fileInfo.getFilePath());
    FileMetadata fileMetadata = createFileMetadata(fileInfo);
    Path path = new Path(fileInfo.getFilePath());

    fileMetadata.setFileName(path.getName());

    FileStatus status = getFileStatus(path);
    fileMetadata.setDirectory(status.isDirectory());
    fileMetadata.setFileLength(status.getLen());

    if (fileInfo.getDirectoryPath() == null) { // Direct filename is given as input.
      fileMetadata.setRelativePath(status.getPath().getName());
    } else {
      String relativePath = getRelativePathWithFolderName(fileInfo);
      fileMetadata.setRelativePath(relativePath);
    }

    if (!status.isDirectory()) {
      int noOfBlocks = (int)((status.getLen() / blockSize) + (((status.getLen() % blockSize) == 0) ? 0 : 1));
      if (fileMetadata.getDataOffset() >= status.getLen()) {
        noOfBlocks = 0;
      }
      fileMetadata.setNumberOfBlocks(noOfBlocks);
      populateBlockIds(fileMetadata);
    }
    return fileMetadata;
  }

  /*
   * As folder name was given to input for copy, prefix folder name to the sub items to copy.
   */
  private String getRelativePathWithFolderName(FileInfo fileInfo)
  {
    String parentDir = new Path(fileInfo.getDirectoryPath()).getName();
    return parentDir + File.separator + fileInfo.getRelativeFilePath();
  }

  /**
   * This can be over-ridden to create file metadata of type that extends {@link FileSplitterInput.FileMetadata}
   *
   * @param fileInfo file information
   * @return file-metadata
   */
  protected FileMetadata createFileMetadata(FileInfo fileInfo)
  {
    return new FileMetadata(fileInfo.getFilePath());
  }

  protected void populateBlockIds(FileMetadata fileMetadata)
  {
    // block ids are 32 bits of operatorId | 32 bits of sequence number
    long[] blockIds = new long[fileMetadata.getNumberOfBlocks()];
    long longLeftSide = ((long)operatorId) << 32;
    for (int i = 0; i < fileMetadata.getNumberOfBlocks(); i++) {
      blockIds[i] = longLeftSide | sequenceNo++ & 0xFFFFFFFFL;
    }
    fileMetadata.setBlockIds(blockIds);
  }

  /**
   * Get default block size which is used when the user hasn't specified block size.
   *
   * @return default block size.
   */
  protected abstract long getDefaultBlockSize();

  /**
   * Get status of a file.
   *
   * @param path path of a file
   * @return file status
   */
  protected abstract FileStatus getFileStatus(Path path) throws IOException;

  public void setBlockSize(Long blockSize)
  {
    this.blockSize = blockSize;
  }

  public Long getBlockSize()
  {
    return blockSize;
  }

  /**
   * Sets number of blocks to be emitted per window.<br/>
   * A lot of blocks emitted per window can overwhelm the downstream operators. Set this value considering blockSize and
   * readersCount.
   * @param threshold
   */
  public void setBlocksThreshold(int threshold)
  {
    this.blocksThreshold = threshold;
  }

  /**
   * Gets number of blocks to be emitted per window.<br/>
   * A lot of blocks emitted per window can overwhelm the downstream operators. Set this value considering blockSize and
   * readersCount.
   * @return
   */
  public int getBlocksThreshold()
  {
    return blocksThreshold;
  }

  /**
   * An {@link Iterator} for Block-Metadatas of a file.
   */
  protected static class BlockMetadataIterator implements Iterator<BlockMetadata.FileBlockMetadata>
  {
    private final FileMetadata fileMetadata;
    private final long blockSize;

    private long pos;
    private int blockNumber;

    private final AbstractFileSplitter splitter;

    protected BlockMetadataIterator()
    {
      //for kryo
      fileMetadata = null;
      blockSize = -1;
      splitter = null;
    }

    protected BlockMetadataIterator(AbstractFileSplitter splitter, FileMetadata fileMetadata, long blockSize)
    {
      this.splitter = splitter;
      this.fileMetadata = fileMetadata;
      this.blockSize = blockSize;
      this.pos = fileMetadata.getDataOffset();
      this.blockNumber = 0;
    }

    @Override
    public boolean hasNext()
    {
      return pos < fileMetadata.getFileLength();
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public BlockMetadata.FileBlockMetadata next()
    {
      long length;
      while ((length = blockSize * ++blockNumber) <= pos) {
      }
      boolean isLast = length >= fileMetadata.getFileLength();
      long lengthOfFileInBlock = isLast ? fileMetadata.getFileLength() : length;
      BlockMetadata.FileBlockMetadata fileBlock = splitter.buildBlockMetadata(pos, lengthOfFileInBlock, blockNumber,
          fileMetadata, isLast);
      pos = lengthOfFileInBlock;
      return fileBlock;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException("remove not supported");
    }
  }

  /**
   * Represents the file metadata - file path, name, no. of blocks, etc.
   */
  public static class FileMetadata
  {
    @NotNull
    private String filePath;
    private String fileName;
    private int numberOfBlocks;
    private long dataOffset;
    private long fileLength;
    private long discoverTime;
    private long[] blockIds;
    private boolean isDirectory;
    private String relativePath;

    @SuppressWarnings("unused")
    protected FileMetadata()
    {
      //for kryo
      filePath = null;
      discoverTime = System.currentTimeMillis();
    }

    /**
     * Constructs file metadata
     *
     * @param filePath file path
     */
    public FileMetadata(@NotNull String filePath)
    {
      this.filePath = filePath;
      discoverTime = System.currentTimeMillis();
    }

    protected FileMetadata(FileMetadata fileMetadata)
    {
      this();
      filePath = fileMetadata.filePath;
      fileName = fileMetadata.fileName;
      numberOfBlocks = fileMetadata.numberOfBlocks;
      dataOffset = fileMetadata.dataOffset;
      fileLength = fileMetadata.fileLength;
      discoverTime = fileMetadata.discoverTime;
      blockIds = fileMetadata.blockIds;
      isDirectory = fileMetadata.isDirectory;
      relativePath = fileMetadata.relativePath;
    }

    /**
     * Returns the total number of blocks.
     */
    public int getNumberOfBlocks()
    {
      return numberOfBlocks;
    }

    /**
     * Sets the total number of blocks.
     */
    public void setNumberOfBlocks(int numberOfBlocks)
    {
      this.numberOfBlocks = numberOfBlocks;
    }

    /**
     * Returns the file name.
     */
    public String getFileName()
    {
      return fileName;
    }

    /**
     * Sets the file name.
     */
    public void setFileName(String fileName)
    {
      this.fileName = fileName;
    }

    /**
     * Sets the file path.
     */
    public void setFilePath(String filePath)
    {
      this.filePath = filePath;
    }

    /**
     * Returns the file path.
     */
    public String getFilePath()
    {
      return filePath;
    }

    /**
     * Returns the data offset.
     */
    public long getDataOffset()
    {
      return dataOffset;
    }

    /**
     * Sets the data offset.
     */
    public void setDataOffset(long offset)
    {
      this.dataOffset = offset;
    }

    /**
     * Returns the file length.
     */
    public long getFileLength()
    {
      return fileLength;
    }

    /**
     * Sets the file length.
     */
    public void setFileLength(long fileLength)
    {
      this.fileLength = fileLength;
    }

    /**
     * Returns the file discover time.
     */
    public long getDiscoverTime()
    {
      return discoverTime;
    }

    /**
     * Sets the discover time.
     */
    public void setDiscoverTime(long discoverTime)
    {
      this.discoverTime = discoverTime;
    }

    /**
     * Returns the block ids associated with the file.
     */
    public long[] getBlockIds()
    {
      return blockIds;
    }

    /**
     * Sets the blocks ids of the file.
     */
    public void setBlockIds(long[] blockIds)
    {
      this.blockIds = blockIds;
    }

    /**
     * Sets whether the file metadata is a directory.
     */
    public void setDirectory(boolean isDirectory)
    {
      this.isDirectory = isDirectory;
    }

    /**
     * @return true if it is a directory; false otherwise.
     */
    public boolean isDirectory()
    {
      return isDirectory;
    }

    /**
     * Sets relative file path
     * @return relativePath
     */
    public String getRelativePath()
    {
      return relativePath;
    }

    /**
     * Gets relative file path
     * @param relativePath
     */
    public void setRelativePath(String relativePath)
    {
      this.relativePath = relativePath;
    }

    @Override
    public String toString()
    {
      return "FileMetadata [fileName=" + fileName + ", numberOfBlocks=" + numberOfBlocks + ", isDirectory=" + isDirectory + ", relativePath=" + relativePath + "]";
    }

  }

  /**
   * A class that encapsulates file path.
   */
  public static class FileInfo
  {
    protected final String directoryPath;
    protected final String relativeFilePath;

    protected FileInfo()
    {
      directoryPath = null;
      relativeFilePath = null;
    }

    public FileInfo(@Nullable String directoryPath, @NotNull String relativeFilePath)
    {
      this.directoryPath = directoryPath;
      this.relativeFilePath = relativeFilePath;
    }

    /**
     * @return directory path
     */
    public String getDirectoryPath()
    {
      return directoryPath;
    }

    /**
     * @return path relative to directory
     */
    public String getRelativeFilePath()
    {
      return relativeFilePath;
    }

    /**
     * @return full path of the file
     */
    public String getFilePath()
    {
      if (directoryPath == null) {
        return relativeFilePath;
      }
      return new Path(directoryPath, relativeFilePath).toUri().getPath();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileSplitter.class);
}
