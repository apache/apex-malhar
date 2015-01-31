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
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OperatorAnnotation;

import com.datatorrent.lib.io.IdempotentStorageManager;

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
public class FileSplitter extends AbstractFileInputOperator<FileSplitter.FileMetadata> implements Operator.CheckpointListener
{
  protected Long blockSize;
  protected transient int operatorId;
  private int sequenceNo;

  @NotNull
  protected IdempotentStorageManager idempotentStorageManager;

  protected transient long currentWindowId;
  protected transient List<String> currentWindowRecoveryState;

  public FileSplitter()
  {
    processedFiles = Sets.newHashSet();
    pendingFiles = Sets.newLinkedHashSet();
    blockSize = null;
    idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
    currentWindowRecoveryState = Lists.newArrayList();
  }

  public final transient DefaultOutputPort<FileMetadata> filesMetadataOutput = new DefaultOutputPort<FileMetadata>();
  public final transient DefaultOutputPort<BlockMetadata> blocksMetadataOutput = new DefaultOutputPort<BlockMetadata>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    assert blockSize == null || blockSize > 0 : "invalid block size";

    operatorId = context.getId();
    if (blockSize == null) {
      blockSize = fs.getDefaultBlockSize(filePath);
    }
    idempotentStorageManager.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    if (windowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      replay(windowId);
    }
    super.beginWindow(windowId);
  }

  protected void replay(long windowId)
  {
    //assumption is that FileSplitter is always statically partitioned. This operator doesn't do
    //much work therefore dynamic partitioning of it is not needed.

    try {
      @SuppressWarnings("unchecked")
      List<String> recoveredData = (List<String>) idempotentStorageManager.load(operatorId, windowId);
      if (recoveredData == null) {
        //This could happen when there are multiple physical instances and one of them is ahead in processing windows.
        return;
      }
      for (String recoveredPath : recoveredData) {
        processedFiles.add(recoveredPath);
        FileMetadata fileMetadata = buildFileMetadata(recoveredPath);
        filesMetadataOutput.emit(fileMetadata);
        Iterator<BlockMetadata> iterator = new BlockMetadataIterator(this, fileMetadata, blockSize);
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
      currentWindowRecoveryState.add(fPath);
      LOG.debug("file {}", fPath);
      try {
        FileMetadata fileMetadata = buildFileMetadata(fPath);
        filesMetadataOutput.emit(fileMetadata);
        Iterator<BlockMetadata> iterator = new BlockMetadataIterator(this, fileMetadata, blockSize);
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

  @Override
  public void endWindow()
  {
    super.endWindow();
    if (currentWindowId > idempotentStorageManager.getLargestRecoveryWindow()) {
      try {
        idempotentStorageManager.save(currentWindowRecoveryState, operatorId, currentWindowId);
      }
      catch (IOException e) {
        throw new RuntimeException("saving recovery", e);
      }
    }
    currentWindowRecoveryState.clear();
  }

  @Override
  public void teardown()
  {
    super.teardown();
    idempotentStorageManager.teardown();
  }

  /**
   * Can be overridden for creating block metadata of a type that extends {@link BlockMetadata}
   */
  protected BlockMetadata createBlockMetadata(long pos, long lengthOfFileInBlock, int blockNumber, FileMetadata fileMetadata, boolean isLast)
  {
    return new BlockMetadata(pos, lengthOfFileInBlock, fileMetadata.getFilePath(), fileMetadata.getBlockIds()[blockNumber - 1], isLast);
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

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    try {
      idempotentStorageManager.deleteUpTo(operatorId, windowId);
    }
    catch (IOException e) {
      throw new RuntimeException("deleting state", e);
    }
  }

  public void setBlockSize(Long blockSize)
  {
    this.blockSize = blockSize;
  }

  public Long getBlockSize()
  {
    return blockSize;
  }

  public void setIdempotentStorageManager(IdempotentStorageManager idempotentStorageManager)
  {
    this.idempotentStorageManager = idempotentStorageManager;
  }

  public IdempotentStorageManager getIdempotentStorageManager()
  {
    return idempotentStorageManager;
  }

  /**
   * An {@link Iterator} for Block-Metadatas of a file.
   */
  public static class BlockMetadataIterator implements Iterator<BlockMetadata>
  {
    private final FileMetadata fileMetadata;
    private final long blockSize;

    private long pos;
    private int blockNumber;

    private FileSplitter splitter;

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
    public BlockMetadata next()
    {
      long length;
      while ((length = blockSize * ++blockNumber) <= pos) {
      }
      boolean isLast = length >= fileMetadata.getFileLength();
      long lengthOfFileInBlock = isLast ? fileMetadata.getFileLength() : length;
      BlockMetadata blockMetadata = splitter.createBlockMetadata(pos, lengthOfFileInBlock, blockNumber, fileMetadata, isLast);
      pos = lengthOfFileInBlock;
      return blockMetadata;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException("remove not supported");
    }
  }

  /**
   * Represent the block metadata - file path, the file offset and length associated with the block and if it is the last
   * block of the file.
   */
  public static class BlockMetadata
  {
    private final long blockId;
    private final String filePath;
    //file offset associated with the block
    private long offset;
    //file length associated with the block
    private long length;
    private final boolean isLastBlock;

    protected BlockMetadata()
    {
      blockId = -1;
      filePath = null;
      offset = -1;
      length = -1;
      isLastBlock = false;
    }

    /**
     * Constructs Block metadata
     *
     * @param offset      offset of the file in the block
     * @param length      length of the file in the block
     * @param filePath    file path
     * @param blockId     block id
     * @param isLastBlock true if this is the last block of file
     */
    public BlockMetadata(long offset, long length, String filePath, long blockId, boolean isLastBlock)
    {
      this.filePath = filePath;
      this.blockId = blockId;
      this.offset = offset;
      this.length = length;
      this.isLastBlock = isLastBlock;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BlockMetadata)) {
        return false;
      }

      BlockMetadata that = (BlockMetadata) o;
      return blockId == that.blockId;
    }

    @Override
    public int hashCode()
    {
      return (int) blockId;
    }

    /**
     * Returns the file path.
     */
    public String getFilePath()
    {
      return filePath;
    }

    /**
     * Returns the block id.
     */
    public long getBlockId()
    {
      return blockId;
    }

    /**
     * Returns the file offset associated with the block.
     */
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

    /**
     * Returns the length of the file in the block.
     */
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

    /**
     * Returns if this is the last block in file.
     */
    public boolean isLastBlock()
    {
      return isLastBlock;
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
