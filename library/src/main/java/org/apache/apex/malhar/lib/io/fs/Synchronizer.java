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

package org.apache.apex.malhar.lib.io.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.block.BlockMetadata;
import org.apache.apex.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import org.apache.apex.malhar.lib.io.fs.AbstractFileSplitter.FileMetadata;
import org.apache.apex.malhar.lib.io.fs.FileStitcher.BlockNotFoundException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Synchronizer waits for all data blocks for a file to be written to disk. It
 * sends trigger to merge the file only after all blocks are written to HDFS
 *
 * @since 3.4.0
 */
public class Synchronizer extends BaseOperator
{
  /**
   * Map to FileMetadata for given file
   */
  private Map<String, FileMetadata> fileMetadataMap = Maps.newHashMap();

  /**
   * Map maintaining BlockId to BlockMetadata mapping for given file
   */
  private Map<String, Map<Long, BlockMetadata.FileBlockMetadata>> fileToReceivedBlocksMetadataMap = Maps.newHashMap();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
  }

  public final transient DefaultInputPort<FileMetadata> filesMetadataInput = new DefaultInputPort<FileMetadata>()
  {
    @Override
    public void process(FileMetadata fileMetadata)
    {
      String filePath = fileMetadata.getFilePath();
      Map<Long, BlockMetadata.FileBlockMetadata> receivedBlocksMetadata = getReceivedBlocksMetadata(filePath);
      fileMetadataMap.put(filePath, fileMetadata);
      emitTriggerIfAllBlocksReceived(fileMetadata, receivedBlocksMetadata);
    }
  };

  public final transient DefaultInputPort<BlockMetadata.FileBlockMetadata> blocksMetadataInput = new DefaultInputPort<BlockMetadata.FileBlockMetadata>()
  {
    @Override
    public void process(BlockMetadata.FileBlockMetadata blockMetadata)
    {
      String filePath = blockMetadata.getFilePath();
      LOG.debug("received blockId {} for file {}", blockMetadata.getBlockId(), filePath);

      Map<Long, BlockMetadata.FileBlockMetadata> receivedBlocksMetadata = getReceivedBlocksMetadata(filePath);
      receivedBlocksMetadata.put(blockMetadata.getBlockId(), blockMetadata);
      FileMetadata fileMetadata = fileMetadataMap.get(filePath);
      if (fileMetadata != null) {
        emitTriggerIfAllBlocksReceived(fileMetadata, receivedBlocksMetadata);
      }

    }
  };

  /**
   * Checks if all blocks for given file are received. Sends triggger when all
   * blocks are received.
   *
   * @param fileMetadata
   * @param receivedBlocksMetadata
   */
  private void emitTriggerIfAllBlocksReceived(FileMetadata fileMetadata,
      Map<Long, BlockMetadata.FileBlockMetadata> receivedBlocksMetadata)
  {

    String filePath = fileMetadata.getFilePath();
    if (receivedBlocksMetadata.size() != fileMetadata.getNumberOfBlocks()) {
      //Some blocks are yet to be received
      fileMetadataMap.put(filePath, fileMetadata);
    } else {
      //No of received blocks match number of file blocks
      Set<Long> receivedBlocks = receivedBlocksMetadata.keySet();

      boolean blockMissing = false;
      if (!fileMetadata.isDirectory()) {
        for (long blockId : fileMetadata.getBlockIds()) {
          if (!receivedBlocks.contains(blockId)) {
            blockMissing = true;
          }
        }
      }

      if (!blockMissing) {
        //All blocks received emit the filemetadata
        long fileProcessingTime = System.currentTimeMillis() - fileMetadata.getDiscoverTime();
        List<StitchBlock> outputBlocks = constructOutputBlockMetadataList(fileMetadata);
        OutputFileMetadata outputFileMetadata = new OutputFileMetadata(fileMetadata, outputBlocks);
        trigger.emit(outputFileMetadata);
        LOG.debug("Total time taken to process the file {} is {} ms", fileMetadata.getFilePath(), fileProcessingTime);
        fileMetadataMap.remove(filePath);
      }
    }
  }

  private List<StitchBlock> constructOutputBlockMetadataList(FileMetadata fileMetadata)
  {
    String filePath = fileMetadata.getFilePath();
    Map<Long, BlockMetadata.FileBlockMetadata> receivedBlocksMetadata = fileToReceivedBlocksMetadataMap.get(filePath);
    List<StitchBlock> outputBlocks = Lists.newArrayList();
    if (fileMetadata.isDirectory()) {
      return outputBlocks;
    }
    long[] blockIDs = fileMetadata.getBlockIds();
    for (int i = 0; i < blockIDs.length; i++) {
      Long blockId = blockIDs[i];
      StitchBlockMetaData outputFileBlockMetaData = new StitchBlockMetaData(receivedBlocksMetadata.get(blockId),
          fileMetadata.getRelativePath(), (i == blockIDs.length - 1));
      outputBlocks.add(outputFileBlockMetaData);
    }
    return outputBlocks;

  }

  private Map<Long, BlockMetadata.FileBlockMetadata> getReceivedBlocksMetadata(String filePath)
  {
    Map<Long, BlockMetadata.FileBlockMetadata> receivedBlocksMetadata = fileToReceivedBlocksMetadataMap.get(filePath);
    if (receivedBlocksMetadata == null) {
      //No blocks received till now
      receivedBlocksMetadata = new HashMap<Long, BlockMetadata.FileBlockMetadata>();
      fileToReceivedBlocksMetadataMap.put(filePath, receivedBlocksMetadata);
    }
    return receivedBlocksMetadata;
  }

  private static final Logger LOG = LoggerFactory.getLogger(Synchronizer.class);
  public final transient DefaultOutputPort<OutputFileMetadata> trigger = new DefaultOutputPort<OutputFileMetadata>();

  /**
   * FileSticher needs information about original sequence of blocks.
   * {@link OutputFileMetadata} maintains list of {@link StitchBlock} in
   * addition to {@link FileMetadata}
   */
  public static class OutputFileMetadata extends FileMetadata implements StitchedFileMetaData
  {
    private List<StitchBlock> stitchBlocksList;

    protected OutputFileMetadata()
    {
      super();
      stitchBlocksList = Lists.newArrayList();
    }

    protected OutputFileMetadata(FileMetadata fileMetaData, List<StitchBlock> stitchBlocksList)
    {
      super(fileMetaData);
      this.stitchBlocksList = stitchBlocksList;
    }

    public OutputFileMetadata(@NotNull String filePath)
    {
      super(filePath);
    }

    public String getStitchedFileRelativePath()
    {
      return getRelativePath();
    }

    @Override
    public List<StitchBlock> getStitchBlocksList()
    {
      return stitchBlocksList;
    }

    /**
     * @param outputBlockMetaDataList
     *          the outputBlockMetaDataList to set
     */
    public void setOutputBlockMetaDataList(List<StitchBlock> outputBlockMetaDataList)
    {
      this.stitchBlocksList = outputBlockMetaDataList;
    }

  }

  /**
   * StitchedFileMetaData is used by FileStitcher to define constituents of the
   * output file.
   */
  public static interface StitchedFileMetaData
  {
    /**
     * @return the outputFileName
     */
    public String getStitchedFileRelativePath();

    public List<StitchBlock> getStitchBlocksList();

  }

  public static interface StitchBlock
  {
    public void writeTo(FileSystem blocksFS, String blocksDir, OutputStream outputStream)
        throws IOException, BlockNotFoundException;

    public long getBlockId();
  }

  /**
   * Partition block representing chunk of data from block files
   */
  public static class StitchBlockMetaData extends FileBlockMetadata implements StitchBlock
  {

    /**
     * Buffer size to be used for reading data from block files
     */
    public static final int BUFFER_SIZE = 64 * 1024;

    /**
     * Relative path of the source file (w.r.t input directory)
     */
    String sourceRelativePath;
    /**
     * Is this the last block for source file
     */
    boolean isLastBlockSource;

    /**
     * Default constructor for serialization
     */
    public StitchBlockMetaData()
    {
      super();
    }

    /**
     * @param fileBlockMetadata
     * @param sourceRelativePath
     * @param isLastBlockSource
     */
    public StitchBlockMetaData(FileBlockMetadata fmd, String sourceRelativePath, boolean isLastBlockSource)
    {
      super(fmd.getFilePath(), fmd.getBlockId(), fmd.getOffset(), fmd.getLength(), fmd.isLastBlock(),
          fmd.getPreviousBlockId());
      this.sourceRelativePath = sourceRelativePath;
      this.isLastBlockSource = isLastBlockSource;
    }

    /**
     * @return the sourceRelativePath
     */
    public String getSourceRelativePath()
    {
      return sourceRelativePath;
    }

    /**
     * @param sourceRelativePath
     *          the sourceRelativePath to set
     */
    public void setSourceRelativePath(String sourceRelativePath)
    {
      this.sourceRelativePath = sourceRelativePath;
    }

    /**
     * @return the isLastBlockSource
     */
    public boolean isLastBlockSource()
    {
      return isLastBlockSource;
    }

    /**
     * @param isLastBlockSource
     *          the isLastBlockSource to set
     */
    public void setLastBlockSource(boolean isLastBlockSource)
    {
      this.isLastBlockSource = isLastBlockSource;
    }

    @Override
    public void writeTo(FileSystem appFS, String blocksDir, OutputStream outputStream)
        throws IOException, BlockNotFoundException
    {
      Path blockPath = new Path(blocksDir, Long.toString(getBlockId()));
      if (!appFS.exists(blockPath)) {
        throw new BlockNotFoundException(blockPath);
      }
      writeTo(appFS, blocksDir, outputStream, 0, appFS.getFileStatus(blockPath).getLen());
    }

    public void writeTo(FileSystem appFS, String blocksDir, OutputStream outputStream, long offset, long length)
        throws IOException, BlockNotFoundException
    {
      InputStream inStream = null;

      byte[] buffer = new byte[BUFFER_SIZE];
      int inputBytesRead;
      Path blockPath = new Path(blocksDir, Long.toString(getBlockId()));
      if (!appFS.exists(blockPath)) {
        throw new BlockNotFoundException(blockPath);
      }
      inStream = appFS.open(blockPath);
      try {
        inStream.skip(offset);

        long bytesRemainingToRead = length;
        int bytesToread = Math.min(BUFFER_SIZE, (int)bytesRemainingToRead);
        while (((inputBytesRead = inStream.read(buffer, 0, bytesToread)) != -1) && bytesRemainingToRead > 0) {
          outputStream.write(buffer, 0, inputBytesRead);
          bytesRemainingToRead -= inputBytesRead;
          bytesToread = Math.min(BUFFER_SIZE, (int)bytesRemainingToRead);
        }
      } finally {
        inStream.close();
      }

    }

  }

}
