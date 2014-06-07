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

import org.apache.hadoop.fs.Path;

/**
 * Adapter for writing to HDFS
 * <p>
 * Serializes tuples into a HDFS file<br>
 * Tuples are written to a single HDFS file or multiple HDFS files, with the option to specify size based file rolling,
 * using place holders in the file path pattern.<br>
 * Example file path pattern : file:///mydir/adviews.out.%(operatorId).part-%(partIndex). where operatorId and partIndex
 * are place holders.
 * </p>
 *
 * @param <T> input port tuple type
 * @since 0.9.4
 */
public abstract class AbstractHdfsRollingFileOutputOperator<T> extends AbstractHdfsFileOutputOperator<T>
{
  protected transient Path currentFilePath;
  /**
   * This variable specifies if the operator needs to close the file at every end window
   */
  protected boolean closeCurrentFile;

  /**
   * @return the closeCurrentFile
   */
  public boolean isCloseCurrentFile()
  {
    return closeCurrentFile;
  }

  /**
   * @param closeCurrentFile
   * the closeCurrentFile to set
   */
  public void setCloseCurrentFile(boolean closeCurrentFile)
  {
    this.closeCurrentFile = closeCurrentFile;
  }

  /**
   * Byte limit for a single file. Once the size is reached, a new file will be created.
   *
   * @param bytesPerFile
   */
  public void setBytesPerFile(int bytesPerFile)
  {
    this.bytesPerFile = bytesPerFile;
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (fsOutput == null) {
      try {
        validateNextFilePath();
        openFile(currentFilePath);
      }
      catch (IOException e) {
        throw new RuntimeException("Failed to open the file.", e);
      }
      currentBytesWritten = 0;
    }
  }

  @Override
  public void endWindow()
  {
    try {
      if (closeCurrentFile) {
        closeFile();
      }
      else {
        if (bufferedOutput != null) {
          bufferedOutput.flush();
        }
        fsOutput.hflush();
      }
    }
    catch (IOException ex) {
      throw new RuntimeException("Failed to flush.", ex);
    }
  }

  @Override
  protected void processTuple(T t)
  {
    try {
      // checks if the stream is open. If not then open a stream
      if (fsOutput == null) {
        validateNextFilePath();
        openFile(currentFilePath);
        currentBytesWritten = 0;
      }
      byte[] tupleBytes = getBytesForTuple(t);
      // checks for the rolling file
      if (bytesPerFile > 0 && currentBytesWritten + tupleBytes.length > bytesPerFile) {
        closeFile();
        validateNextFilePath();
        openFile(currentFilePath);
        currentBytesWritten = 0;
      }
      if (bufferedOutput != null) {
        bufferedOutput.write(tupleBytes);
      }
      else {
        fsOutput.write(tupleBytes);
      }
      currentBytesWritten += tupleBytes.length;
      totalBytesWritten += tupleBytes.length;
    }
    catch (IOException ex) {
      throw new RuntimeException("Failed to write to stream.", ex);
    }
  }

  /**
   * This checks if the new path is not same as old path. If it is then throw exception
   */
  private void validateNextFilePath()
  {
    Path filepath = nextFilePath();
    if (currentFilePath != null && filepath.equals(currentFilePath)) {
      throw new IllegalArgumentException("Rolling files require %() placeholders for unique names: " + filepath);
    }
    currentFilePath = filepath;
  }

  /**
   * This function returns the path for the file output. If the implementing class wants to use single file, then it can
   * return the same path every time o/w different based on the use case
   *
   *
   * @return
   */
  public abstract Path nextFilePath();

}
