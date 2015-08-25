/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * An abstract File Splitter.
 *
 * @param <SCANNER> type of scanner
 */
public abstract class AbstractFileSplitter<SCANNER extends AbstractFileSplitter.Scanner> extends BaseOperator
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

  protected Iterator<BlockMetadata.FileBlockMetadata> blockMetadataIterator;

  @NotNull
  protected SCANNER scanner;

  protected transient int operatorId;
  protected transient Context.OperatorContext context;
  protected transient long currentWindowId;

  protected final BasicCounters<MutableLong> fileCounters;

  public final transient DefaultOutputPort<FileMetadata> filesMetadataOutput = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<BlockMetadata.FileBlockMetadata> blocksMetadataOutput = new DefaultOutputPort<>();

  public AbstractFileSplitter()
  {
    fileCounters = new BasicCounters<>(MutableLong.class);
    blocksThreshold = Integer.MAX_VALUE;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    Preconditions.checkArgument(blockSize == null || blockSize > 0, "invalid block size");

    operatorId = context.getId();
    this.context = context;

    fileCounters.setCounter(Counters.PROCESSED_FILES, new MutableLong());

    scanner.setup(context);

    if (blockSize == null) {
      blockSize = getDefaultBlockSize();
    }
  }

  @Override
  public void teardown()
  {
    try {
      scanner.teardown();
    } catch (Throwable t) {
      DTThrowable.rethrow(t);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    blockCount = 0;
    currentWindowId = windowId;
  }

  /**
   * @param fileInfo file info
   * @return true if blocks threshold is reached; false otherwise
   */
  protected boolean process(FileInfo fileInfo)
  {
    try {
      FileMetadata fileMetadata = buildFileMetadata(fileInfo);
      filesMetadataOutput.emit(fileMetadata);
      fileCounters.getCounter(Counters.PROCESSED_FILES).increment();
      if (!fileMetadata.isDirectory()) {
        blockMetadataIterator = new BlockMetadataIterator(this, fileMetadata, blockSize);
        if (!emitBlockMetadata()) {
          //block threshold reached
          return false;
        }
      }
      if (fileInfo.lastFileOfScan) {
        return false;
      }
    } catch (IOException e) {
      throw new RuntimeException("creating metadata", e);
    }
    return true;
  }

  @Override
  public void endWindow()
  {
    context.setCounters(fileCounters);
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
   * Can be overridden for creating block metadata of a type that extends {@link BlockMetadata.FileBlockMetadata}
   */
  protected BlockMetadata.FileBlockMetadata createBlockMetadata(long pos, long lengthOfFileInBlock, int blockNumber,
                                                                FileMetadata fileMetadata, boolean isLast)
  {
    return new BlockMetadata.FileBlockMetadata(fileMetadata.getFilePath(), fileMetadata.getBlockIds()[blockNumber - 1], pos,
      lengthOfFileInBlock, isLast, blockNumber == 1 ? -1 : fileMetadata.getBlockIds()[blockNumber - 2]);

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

    if (!status.isDirectory()) {
      int noOfBlocks = (int) ((status.getLen() / blockSize) + (((status.getLen() % blockSize) == 0) ? 0 : 1));
      if (fileMetadata.getDataOffset() >= status.getLen()) {
        noOfBlocks = 0;
      }
      fileMetadata.setNumberOfBlocks(noOfBlocks);
      populateBlockIds(fileMetadata);
    }
    return fileMetadata;
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
    long longLeftSide = ((long) operatorId) << 32;
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

  public void setBlocksThreshold(int threshold)
  {
    this.blocksThreshold = threshold;
  }

  public int getBlocksThreshold()
  {
    return blocksThreshold;
  }

  public void setScanner(SCANNER scanner)
  {
    this.scanner = scanner;
  }

  public SCANNER getScanner()
  {
    return this.scanner;
  }

  @InterfaceStability.Evolving
  interface Scanner extends Component<Context.OperatorContext>
  {
    /**
     * @return information of a file discovered
     */
    FileInfo pollFile();
  }

  /**
   * An {@link Iterator} for Block-Metadatas of a file.
   */
  public static class BlockMetadataIterator implements Iterator<BlockMetadata.FileBlockMetadata>
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

    public BlockMetadataIterator(AbstractFileSplitter splitter, FileMetadata fileMetadata, long blockSize)
    {
      this.splitter = splitter;
      this.fileMetadata = fileMetadata;
      this.blockSize = blockSize;
      this.pos = fileMetadata.getDataOffset();
      this.blockNumber = 0;
    }

    @Deprecated
    public BlockMetadataIterator(FileSplitterInput splitter, FileMetadata fileMetadata, long blockSize)
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
      BlockMetadata.FileBlockMetadata fileBlock = splitter.createBlockMetadata(pos, lengthOfFileInBlock, blockNumber, fileMetadata, isLast);
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
  }

  /**
   * A class that represents the file discovered by time-based scanner.
   */
  protected static class FileInfo
  {
    protected final String directoryPath;
    protected final String relativeFilePath;
    protected final long modifiedTime;
    protected transient boolean lastFileOfScan;

    private FileInfo()
    {
      directoryPath = null;
      relativeFilePath = null;
      modifiedTime = -1;
    }

    protected FileInfo(String filePath, long modifiedTime)
    {
      this(null, filePath, modifiedTime);
    }

    protected FileInfo(@Nullable String directoryPath, @NotNull String relativeFilePath, long modifiedTime)
    {
      this.directoryPath = directoryPath;
      this.relativeFilePath = relativeFilePath;
      this.modifiedTime = modifiedTime;
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

    public boolean isLastFileOfScan()
    {
      return lastFileOfScan;
    }
  }

  public enum Counters
  {
    PROCESSED_FILES
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileSplitter.class);
}
