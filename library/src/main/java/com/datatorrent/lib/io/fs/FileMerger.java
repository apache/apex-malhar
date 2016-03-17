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

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.Synchronizer.OutputFileMetadata;

/**
 * This operator merges the blocks into a file. The list of blocks is obtained
 * from the OutputFileMetadata. The implementation extends FileStitcher (which
 * uses reconciler), hence the file merging operation is carried out in a
 * separate thread.
 *
 */
public class FileMerger extends FileStitcher<OutputFileMetadata>
{
  /**
   * Flag to control if existing file with same name should be overwritten
   */
  private boolean overwriteOnConflict = true;

  private static final Logger LOG = LoggerFactory.getLogger(FileMerger.class);

  @AutoMetric
  private long bytesWrittenPerSec;

  private long bytesWritten;
  private double windowTimeSec;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT)
        * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    bytesWrittenPerSec = 0;
    bytesWritten = 0;
  }

  /* 
   * Calls super.endWindow() and sets counters 
   * @see com.datatorrent.api.BaseOperator#endWindow()
   */
  @Override
  public void endWindow()
  {
    OutputFileMetadata outputFileMetadata;
    int size = doneTuples.size();
    for (int i = 0; i < size; i++) {
      outputFileMetadata = doneTuples.peek();
      // If a tuple is present in doneTuples, it has to be also present in successful/failed/skipped
      // as processCommittedData adds tuple in successful/failed/skipped
      // and then reconciler thread add that in doneTuples 
      if (successfulFiles.contains(outputFileMetadata)) {
        successfulFiles.remove(outputFileMetadata);
        LOG.debug("File copy successful: {}", outputFileMetadata.getStitchedFileRelativePath());
      } else if (skippedFiles.contains(outputFileMetadata)) {
        skippedFiles.remove(outputFileMetadata);
        LOG.debug("File copy skipped: {}", outputFileMetadata.getStitchedFileRelativePath());
      } else if (failedFiles.contains(outputFileMetadata)) {
        failedFiles.remove(outputFileMetadata);
        LOG.debug("File copy failed: {}", outputFileMetadata.getStitchedFileRelativePath());
      } else {
        throw new RuntimeException("Tuple present in doneTuples but not in successfulFiles: "
            + outputFileMetadata.getStitchedFileRelativePath());
      }
      completedFilesMetaOutput.emit(outputFileMetadata);
      committedTuples.remove(outputFileMetadata);
      doneTuples.poll();
    }

    bytesWrittenPerSec = (long)(bytesWritten / windowTimeSec);
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

  public boolean isOverwriteOnConflict()
  {
    return overwriteOnConflict;
  }

  public void setOverwriteOnConflict(boolean overwriteOnConflict)
  {
    this.overwriteOnConflict = overwriteOnConflict;
  }
}
