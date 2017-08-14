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
import java.io.OutputStream;

import org.apache.apex.malhar.lib.io.fs.Synchronizer.OutputFileMetadata;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.AutoMetric;

/**
 * This operator merges the blocks into a file. The list of blocks is obtained
 * from the OutputFileMetadata. The implementation extends FileStitcher (which
 * uses reconciler), hence the file merging operation is carried out in a
 * separate thread.
 *
 *
 * @since 3.4.0
 */
public class FileMerger extends FileStitcher<OutputFileMetadata>
{
  /**
   * Flag to control if existing file with same name should be overwritten
   */
  private boolean overwriteOnConflict = true;

  @AutoMetric
  private long bytesWritten;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    bytesWritten = 0;
  }

  @Override
  protected void mergeOutputFile(OutputFileMetadata outputFileMetadata) throws IOException
  {
    LOG.debug("Processing file: {}", outputFileMetadata.getStitchedFileRelativePath());

    Path outputFilePath = new Path(filePath, outputFileMetadata.getStitchedFileRelativePath());
    if (outputFileMetadata.isDirectory()) {
      createDir(outputFilePath);
      successfulFiles.add(outputFileMetadata);
      return;
    }

    if (outputFS.exists(outputFilePath) && !overwriteOnConflict) {
      LOG.debug("Output file {} already exits and overwrite flag is off. Skipping.", outputFilePath);
      skippedFiles.add(outputFileMetadata);
      return;
    }
    //Call super method for serial merge of blocks
    super.mergeOutputFile(outputFileMetadata);
    deleteBlockFiles(outputFileMetadata);
  }

  /**
   * @param tuple
   */
  private void deleteBlockFiles(OutputFileMetadata outputFileMetadata)
  {
    if (outputFileMetadata.isDirectory()) {
      return;
    }

    for (long blockId : outputFileMetadata.getBlockIds()) {
      Path blockPath = new Path(blocksDirectoryPath, Long.toString(blockId));
      try {
        if (appFS.exists(blockPath)) { // takes care if blocks are deleted and then the operator is redeployed.
          appFS.delete(blockPath, false);
        }
      } catch (IOException e) {
        throw new RuntimeException("Unable to delete block: " + blockId, e);
      }
    }
  }

  @Override
  protected OutputStream writeTempOutputFile(OutputFileMetadata outputFileMetadata)
      throws IOException, BlockNotFoundException
  {
    OutputStream outputStream = super.writeTempOutputFile(outputFileMetadata);
    bytesWritten += outputFileMetadata.getFileLength();
    return outputStream;
  }

  private void createDir(Path outputFilePath) throws IOException
  {
    if (!outputFS.exists(outputFilePath)) {
      outputFS.mkdirs(outputFilePath);
    }
  }

  @Override
  protected OutputStream getOutputStream(Path partFilePath) throws IOException
  {
    OutputStream outputStream = outputFS.create(partFilePath);
    return outputStream;
  }

  /**
   * Flag to control if existing file with same name should be overwritten
   * @return Flag to control if existing file with same name should be overwritten
   */
  public boolean isOverwriteOnConflict()
  {
    return overwriteOnConflict;
  }

  /**
   * Flag to control if existing file with same name should be overwritten
   * @param overwriteOnConflict Flag to control if existing file with same name should be overwritten
   */
  public void setOverwriteOnConflict(boolean overwriteOnConflict)
  {
    this.overwriteOnConflict = overwriteOnConflict;
  }
}
