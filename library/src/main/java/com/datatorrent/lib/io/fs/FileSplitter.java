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

import javax.validation.constraints.NotNull;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Input operator that scans a directory for files and splits a file into blocks.<br/>
 * The operator emits block metadata and file metadata.<br/>
 */
public class FileSplitter extends AbstractFSDirectoryInputOperator<FileSplitter.FileMetadata>
{
  protected Long blockSize;
  protected transient int operatorId;
  private long sequenceNo;

  public FileSplitter()
  {
    processedFiles = Sets.newHashSet();
    pendingFiles = Sets.newLinkedHashSet();
    blockSize = null;
  }

  public final transient DefaultOutputPort<FileMetadata> filesMetadataOutput = new DefaultOutputPort<FileMetadata>();
  public final transient DefaultOutputPort<BlockMetadata> blocksMetadataOutput = new DefaultOutputPort<BlockMetadata>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    operatorId = context.getId();
    if (blockSize != null && blockSize < 0) {
      throw new RuntimeException("negative block size " + blockSize);
    }
    if (blockSize == null) {
      blockSize = fs.getDefaultBlockSize(filePath);
    }
  }

  @Override
  public void emitTuples()
  {
    scanDirectory();

    Iterator<String> pendingIterator = pendingFiles.iterator();
    while (pendingIterator.hasNext()) {
      String filePath = pendingIterator.next();
      LOG.debug("file {}", filePath);
      try {
        FileMetadata fileMetadata = buildFileMetadata(filePath);
        filesMetadataOutput.emit(fileMetadata);
        Iterator<BlockMetadata> iterator = getBlockMetadataIterator(fileMetadata);
        while (iterator.hasNext()) {
          this.blocksMetadataOutput.emit(iterator.next());
        }
      }
      catch (IOException e) {
        throw new RuntimeException("creating metadata", e);
      }
      pendingIterator.remove();
      processedFiles.add(filePath);
    }
  }

  protected Iterator<BlockMetadata> getBlockMetadataIterator(FileMetadata metadata)
  {
    return new BlockMetadataIterator(metadata, blockSize);
  }

  @Override
  protected FileMetadata readEntity() throws IOException
  {
    return new FileMetadata(currentFile);
  }

  /**
   * Creates file metadata and populates no. of blocks in the metadata.
   *
   * @param filePath file-path
   * @return file-metadata
   * @throws IOException
   */
  protected FileMetadata buildFileMetadata(String filePath) throws IOException
  {
    currentFile = filePath;
    Path path = new Path(filePath);

    FileMetadata fileMetadata = readEntity();
    fileMetadata.setFileName(path.getName());

    FileStatus status = fs.getFileStatus(path);
    int noOfBlocks = (int) Math.ceil(status.getLen() / (blockSize * 1.0));
    if (fileMetadata.getDataOffset() >= status.getLen()) {
      noOfBlocks = 0;
    }
    fileMetadata.setFileLength(status.getLen());
    fileMetadata.setNumberOfBlocks(noOfBlocks);
    populateBlockIds(fileMetadata);
    return fileMetadata;
  }

  @Override
  public void teardown()
  {
    try {
      fs.close();
    }
    catch (IOException ex) {
      throw new RuntimeException("closing fs", ex);
    }
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
  public static class BlockMetadataIterator implements Iterator<BlockMetadata>
  {
    private final FileMetadata fileMetadata;
    private final long blockSize;

    private long pos;
    private int blockNumber;

    public BlockMetadataIterator(FileMetadata fileMetadata, long blockSize)
    {
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
      BlockMetadata blockMetadata = new BlockMetadata(pos, lengthOfFileInBlock, fileMetadata.getFilePath(), fileMetadata.getBlockIds()[blockNumber - 1], isLast);
      pos = lengthOfFileInBlock;
      return blockMetadata;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException("remove not supported");
    }
  }

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

    public String getFilePath()
    {
      return filePath;
    }

    public long getBlockId()
    {
      return blockId;
    }

    public long getOffset()
    {
      return offset;
    }

    public void setOffset(long offset)
    {
      this.offset = offset;
    }

    public long getLength()
    {
      return length;
    }

    public void setLength(long length)
    {
      this.length = length;
    }

    public boolean isLastBlock()
    {
      return isLastBlock;
    }
  }

  public static class FileMetadata
  {
    @NotNull
    private String filePath;
    private String fileName;
    private int numberOfBlocks;
    private long dataOffset;
    private long fileLength;
    private final long creationTime;
    private long[] blockIds;

    protected FileMetadata()
    {
      filePath = null;
      creationTime = System.currentTimeMillis();
    }

    public FileMetadata(@NotNull String filePath)
    {
      this.filePath = filePath;
      creationTime = System.currentTimeMillis();
    }

    public int getNumberOfBlocks()
    {
      return numberOfBlocks;
    }

    public void setNumberOfBlocks(int numberOfBlocks)
    {
      this.numberOfBlocks = numberOfBlocks;
    }

    public String getFileName()
    {
      return fileName;
    }

    public void setFileName(String fileName)
    {
      this.fileName = fileName;
    }

    public void setFilePath(String filePath)
    {
      this.filePath = filePath;
    }

    public String getFilePath()
    {
      return filePath;
    }

    public long getDataOffset()
    {
      return dataOffset;
    }

    public void setDataOffset(long offset)
    {
      this.dataOffset = offset;
    }

    public long getFileLength()
    {
      return fileLength;
    }

    public void setFileLength(long fileLength)
    {
      this.fileLength = fileLength;
    }

    public long getCreationTime()
    {
      return creationTime;
    }

    public long[] getBlockIds()
    {
      return blockIds;
    }

    public void setBlockIds(long[] blockIds)
    {
      this.blockIds = blockIds;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileSplitter.class);
}