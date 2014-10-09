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

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;

/**
 * This operator writes out tuples to hdfs while obeying the exactly once constraint.
 * <p>
 * The Operator creates file <window_id>.tmp during beginwindow and writes the tuples to it.
 * It moves the file to <window_id> in the end window.
 * If the operator fails and recovers, checks if the file <window_id> exists during begin window. If it does,
 * then the operator doesn't process anything during that window. If it doesn't, then the operator deletes
 * the <window_id>.tmp file if it exists, creates new and starts writing to it.
 * </p>
 * @displayName HDFS Exactly Once Output
 * @category Output
 * @tags hdfs, files, output operator
 *
 * @since 1.0.2
 */
public class HdfsExactlyOnceOutputOperator extends AbstractHdfsFileOutputOperator<String>
{
  private final String TEMP = ".tmp";
  private transient Path currentFilePath;
  private transient Path currentTempFilePath;

  @Override
  protected void processTuple(String t)
  {
    try {
      // if stream is not open, then do nothing since the file already exists for current window
      if (fsOutput == null) {
        return;
      }

      byte[] tupleBytes = getBytesForTuple(t);

      if (bufferedOutput != null) {
        bufferedOutput.write(tupleBytes);
      }
      else {
        fsOutput.write(tupleBytes);
      }
      totalBytesWritten += tupleBytes.length;
    }
    catch (IOException ex) {
      throw new RuntimeException("Failed to write to stream.", ex);
    }

  }

  @Override
  public void beginWindow(long windowId)
  {
    try {
      currentFilePath = new Path(filePath + "/" + windowId);
      currentTempFilePath = currentFilePath.suffix(TEMP);
      if (fs.exists(currentFilePath)) {
        fsOutput = null;
      }
      else {
        if (fs.exists(currentTempFilePath)) {
          fs.delete(currentTempFilePath, true);
        }
        openFile(currentTempFilePath);
      }
    }
    catch (IOException e) {
      throw new RuntimeException("Failed to open the file.", e);
    }
  }

  @Override
  public void endWindow()
  {
    if (fsOutput != null) {
      try {
        closeFile();
        fs.rename(currentTempFilePath, currentFilePath);
      }
      catch (IOException ex) {
        throw new RuntimeException("Failed to flush.", ex);
      }
    }
  }

  @Override
  protected byte[] getBytesForTuple(String t)
  {
    return (t + "\n").getBytes();
  }

  @Override
  public void setAppend(boolean append)
  {
    append = false;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    append = false;
  }

  @Override
  public void teardown()
  {
    super.teardown();
    fsOutput = null;
  }

  private static final long serialVersionUID = 201405201214L;
  private static final Logger logger = LoggerFactory.getLogger(HdfsExactlyOnceOutputOperator.class);
}
