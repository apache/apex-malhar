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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.fs.Synchronizer.OutputFileMetadata;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.Context.OperatorContext;

/**
 * HDFS file merger extends file merger to optimize for HDFS file copy usecase.
 * This uses fast merge from HDFS if destination filesystem is same as
 * application filesystem.
 *
 * @since 3.4.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class HDFSFileMerger extends FileMerger
{
  /**
   * Fast merge is possible if append is allowed for output file system.
   */
  private transient boolean fastMergeActive;
  /**
   * Default block size for output file system
   */
  private transient long defaultBlockSize;
  /**
   * Decision maker to enable fast merge based on blocks directory, application
   * directory, block size
   */
  private transient FastMergerDecisionMaker fastMergerDecisionMaker;

  /**
   * Initializations based on output file system configuration
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    fastMergeActive = outputFS.getConf().getBoolean("dfs.support.append", true)
        && appFS.getUri().equals(outputFS.getUri());
    LOG.debug("appFS.getUri():{}", appFS.getUri());
    LOG.debug("outputFS.getUri():{}", outputFS.getUri());
    defaultBlockSize = outputFS.getDefaultBlockSize(new Path(filePath));
    fastMergerDecisionMaker = new FastMergerDecisionMaker(blocksDirectoryPath, appFS, defaultBlockSize);
  }

  /**
   * Uses fast merge if possible. Else, fall back to serial merge.
   */
  @Override
  protected void mergeBlocks(OutputFileMetadata outputFileMetadata) throws IOException
  {

    try {
      LOG.debug("fastMergeActive: {}", fastMergeActive);
      if (fastMergeActive && fastMergerDecisionMaker.isFastMergePossible(outputFileMetadata)
          && outputFileMetadata.getNumberOfBlocks() > 0) {
        LOG.debug("Using fast merge on HDFS.");
        concatBlocks(outputFileMetadata);
        return;
      }
      LOG.debug("Falling back to slow merge on HDFS.");
      super.mergeBlocks(outputFileMetadata);

    } catch (BlockNotFoundException e) {
      if (recover(outputFileMetadata)) {
        LOG.debug("Recovery attempt successful.");
        successfulFiles.add(outputFileMetadata);
      } else {
        failedFiles.add(outputFileMetadata);
      }
    }
  }

  /**
   * Fast merge using HDFS block concat
   *
   * @param outputFileMetadata
   * @throws IOException
   */
  private void concatBlocks(OutputFileMetadata outputFileMetadata) throws IOException
  {
    Path outputFilePath = new Path(filePath, outputFileMetadata.getRelativePath());

    int numBlocks = outputFileMetadata.getNumberOfBlocks();
    long[] blocksArray = outputFileMetadata.getBlockIds();

    Path firstBlock = new Path(blocksDirectoryPath, Long.toString(blocksArray[0]));
    if (numBlocks > 1) {
      Path[] blockFiles = new Path[numBlocks - 1]; // Leave the first block

      for (int index = 1; index < numBlocks; index++) {
        blockFiles[index - 1] = new Path(blocksDirectoryPath, Long.toString(blocksArray[index]));
      }

      outputFS.concat(firstBlock, blockFiles);
    }

    moveToFinalFile(firstBlock, outputFilePath);
  }

  /**
   * Attempt for recovery if block concat is successful but temp file is not
   * moved to final file
   *
   * @param outputFileMetadata
   * @throws IOException
   */
  @VisibleForTesting
  protected boolean recover(OutputFileMetadata outputFileMetadata) throws IOException
  {
    Path firstBlockPath = new Path(blocksDirectoryPath + Path.SEPARATOR + outputFileMetadata.getBlockIds()[0]);
    Path outputFilePath = new Path(filePath, outputFileMetadata.getRelativePath());
    if (appFS.exists(firstBlockPath)) {
      FileStatus status = appFS.getFileStatus(firstBlockPath);
      if (status.getLen() == outputFileMetadata.getFileLength()) {
        moveToFinalFile(firstBlockPath, outputFilePath);
        return true;
      }
      LOG.error("Unable to recover in FileMerger for file: {}", outputFilePath);
      return false;
    }

    if (outputFS.exists(outputFilePath)) {
      LOG.debug("Output file already present at the destination, nothing to recover.");
      return true;
    }
    LOG.error("Unable to recover in FileMerger for file: {}", outputFilePath);
    return false;
  }

  private static final Logger LOG = LoggerFactory.getLogger(HDFSFileMerger.class);

  /**
   * Utility class to decide fast merge possibility
   */
  public static class FastMergerDecisionMaker
  {

    private String blocksDir;
    private FileSystem appFS;
    private long defaultBlockSize;

    public FastMergerDecisionMaker(String blocksDir, FileSystem appFS, long defaultBlockSize)
    {
      this.blocksDir = blocksDir;
      this.appFS = appFS;
      this.defaultBlockSize = defaultBlockSize;
    }

    /**
     * Checks if fast merge is possible for given settings for blocks directory,
     * application file system, block size
     *
     * @param outputFileMetadata
     * @throws IOException
     * @throws BlockNotFoundException
     */
    public boolean isFastMergePossible(OutputFileMetadata outputFileMetadata) throws IOException, BlockNotFoundException
    {
      short replicationFactor = 0;
      boolean sameReplicationFactor = true;
      boolean multipleOfBlockSize = true;

      int numBlocks = outputFileMetadata.getNumberOfBlocks();
      LOG.debug("fileMetadata.getNumberOfBlocks(): {}", outputFileMetadata.getNumberOfBlocks());
      long[] blocksArray = outputFileMetadata.getBlockIds();
      LOG.debug("fileMetadata.getBlockIds().len: {}", outputFileMetadata.getBlockIds().length);

      for (int index = 0; index < numBlocks && (sameReplicationFactor && multipleOfBlockSize); index++) {
        Path blockFilePath = new Path(blocksDir + Path.SEPARATOR + blocksArray[index]);
        if (!appFS.exists(blockFilePath)) {
          throw new BlockNotFoundException(blockFilePath);
        }
        FileStatus status = appFS.getFileStatus(new Path(blocksDir + Path.SEPARATOR + blocksArray[index]));
        if (index == 0) {
          replicationFactor = status.getReplication();
          LOG.debug("replicationFactor: {}", replicationFactor);
        } else {
          sameReplicationFactor = (replicationFactor == status.getReplication());
          LOG.debug("sameReplicationFactor: {}", sameReplicationFactor);
        }

        if (index != numBlocks - 1) {
          multipleOfBlockSize = (status.getLen() % defaultBlockSize == 0);
          LOG.debug("multipleOfBlockSize: {}", multipleOfBlockSize);
        }
      }
      return sameReplicationFactor && multipleOfBlockSize;
    }
  }

}
