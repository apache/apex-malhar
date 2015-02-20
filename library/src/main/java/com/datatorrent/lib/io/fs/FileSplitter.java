/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.fs;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;

/**
 * Input operator that scans a directory for files and splits a file into blocks.<br/>
 * The operator emits block metadata and file metadata.<br/>
 *
 * @displayName File Splitter
 * @category Input
 * @tags file, input operator
 *
 * @since 2.0.0
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class FileSplitter extends AbstractFileInputOperator<FileSplitter.FileMetadata>
{
  protected Long blockSize;
  protected transient int operatorId;
  private int sequenceNo;

  protected transient long currentWindowId;

  public FileSplitter()
  {
    processedFiles = Sets.newHashSet();
    pendingFiles = Sets.newLinkedHashSet();
    blockSize = null;
    idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
  }

  public final transient DefaultOutputPort<FileMetadata> filesMetadataOutput = new DefaultOutputPort<FileMetadata>();
  public final transient DefaultOutputPort<FileBlockMetadata> blocksMetadataOutput = new DefaultOutputPort<FileBlockMetadata>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    assert blockSize == null || blockSize > 0 : "invalid block size";

    operatorId = context.getId();
    if (blockSize == null) {
      blockSize = fs.getDefaultBlockSize(filePath);
    }
  }

  @Override
  protected void replay(long windowId)
  {
    //assumption is that FileSplitter is always statically partitioned. This operator doesn't do
    //much work therefore dynamic partitioning of it is not needed.

    try {
      @SuppressWarnings("unchecked")
      LinkedList<RecoveryEntry> recoveredData = (LinkedList<RecoveryEntry>) idempotentStorageManager.load(operatorId, windowId);
      if (recoveredData == null) {
        //This could happen when there are multiple physical instances and one of them is ahead in processing windows.
        return;
      }
      for (RecoveryEntry entry : recoveredData) {
        processedFiles.add(entry.file);
        FileMetadata fileMetadata = buildFileMetadata(entry.file);
        filesMetadataOutput.emit(fileMetadata);
        Iterator<FileBlockMetadata> iterator = new BlockMetadataIterator(this, fileMetadata, blockSize);
        while (iterator.hasNext()) {
          this.blocksMetadataOutput.emit(iterator.next());
        }
      }
    }
    catch (IOException e) {
      throw new RuntimeException("replay", e);
    }
  }

  @Override
  public void emitTuples()
  {
    if (currentWindowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      return;
    }
    //This adds the files to processed and pending sets
    scanDirectory();

    Iterator<String> pendingIterator = pendingFiles.iterator();
    while (pendingIterator.hasNext()) {
      String fPath = pendingIterator.next();
      currentWindowRecoveryState.add(new RecoveryEntry(fPath, 0, 0));
      LOG.debug("file {}", fPath);
      try {
        FileMetadata fileMetadata = buildFileMetadata(fPath);
        filesMetadataOutput.emit(fileMetadata);
        Iterator<FileBlockMetadata> iterator = new BlockMetadataIterator(this, fileMetadata, blockSize);
        while (iterator.hasNext()) {
          this.blocksMetadataOutput.emit(iterator.next());
        }
      }
      catch (IOException e) {
        throw new RuntimeException("creating metadata", e);
      }
      pendingIterator.remove();
    }
  }

  /**
   * Can be overridden for creating block metadata of a type that extends {@link FileBlockMetadata}
   */
  protected FileBlockMetadata createBlockMetadata(long pos, long lengthOfFileInBlock, int blockNumber, FileMetadata fileMetadata, boolean isLast)
  {
    return new FileBlockMetadata(fileMetadata.getFilePath(), fileMetadata.getBlockIds()[blockNumber - 1], pos,
      lengthOfFileInBlock, isLast, blockNumber == 1 ? -1 : fileMetadata.getBlockIds()[blockNumber - 2]);

  }

  @Override
  protected FileMetadata readEntity() throws IOException
  {
    return new FileMetadata(currentFile);
  }

  /**
   * Creates file metadata and populates no. of blocks in the metadata.
   *
   * @param fPath file-path
   * @return file-metadata
   * @throws IOException
   */
  protected FileMetadata buildFileMetadata(String fPath) throws IOException
  {
    currentFile = fPath;
    Path path = new Path(fPath);

    FileMetadata fileMetadata = readEntity();
    fileMetadata.setFileName(path.getName());

    FileStatus status = fs.getFileStatus(path);
    int noOfBlocks = (int) ((status.getLen() / blockSize) + (((status.getLen() % blockSize) == 0) ? 0 : 1));
    if (fileMetadata.getDataOffset() >= status.getLen()) {
      noOfBlocks = 0;
    }
    fileMetadata.setFileLength(status.getLen());
    fileMetadata.setNumberOfBlocks(noOfBlocks);
    populateBlockIds(fileMetadata);
    return fileMetadata;
  }

  protected void populateBlockIds(FileMetadata fileMetadata)
  {
    // block ids are 32 bits of operatorId | 32 bits of sequence number
    long[] blockIds = new long[fileMetadata.getNumberOfBlocks()];
    long longLeftSide = ((long) operatorId) << 32;
    for (int i = 0; i < fileMetadata.getNumberOfBlocks(); i++) {
      blockIds[i] = longLeftSide | sequenceNo++ & 0xFFFFFFFFL;
    }
    fileMetadata.setBlockIds(blockIds);
  }

  @Override
  protected void emit(FileMetadata tuple)
  {
    throw new UnsupportedOperationException("not supported");
  }

  public void setBlockSize(Long blockSize)
  {
    this.blockSize = blockSize;
  }

  public Long getBlockSize()
  {
    return blockSize;
  }

  /**
   * An {@link Iterator} for Block-Metadatas of a file.
   */
  public static class BlockMetadataIterator implements Iterator<FileBlockMetadata>
  {
    private final FileMetadata fileMetadata;
    private final long blockSize;

    private long pos;
    private int blockNumber;

    private final FileSplitter splitter;

    public BlockMetadataIterator(FileSplitter splitter, FileMetadata fileMetadata, long blockSize)
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
    public FileBlockMetadata next()
    {
      long length;
      while ((length = blockSize * ++blockNumber) <= pos) {
      }
      boolean isLast = length >= fileMetadata.getFileLength();
      long lengthOfFileInBlock = isLast ? fileMetadata.getFileLength() : length;
      FileBlockMetadata fileBlock = splitter.createBlockMetadata(pos, lengthOfFileInBlock, blockNumber, fileMetadata, isLast);
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

    protected FileMetadata()
    {
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
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileSplitter.class);
}
