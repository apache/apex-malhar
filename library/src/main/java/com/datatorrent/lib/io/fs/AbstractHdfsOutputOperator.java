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

import java.io.BufferedOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;

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
 * 
 */
public abstract class AbstractHdfsOutputOperator<T> extends BaseOperator
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractHdfsOutputOperator.class);
  private transient FSDataOutputStream fsOutput;
  private transient BufferedOutputStream bufferedOutput;
  private transient FileSystem fs;
  // internal persistent state
  private int currentBytesWritten = 0;
  private long totalBytesWritten = 0;

  private String filePathPattern;
  private boolean append = true;
  private int bufferSize = 0;
  private int bytesPerFile = 0;
  private int replication = 0;
  private Path currentFilePath;
  /**
   * This variable specifies if the operator needs to close the file at every end window
   */
  private boolean closeCurrentFile;

  /**
   * The file name. This can be a relative path for the default file system or fully qualified URL as accepted by (
   * {@link org.apache.hadoop.fs.Path}). For splits with per file size limit, the name needs to contain substitution
   * tokens to generate unique file names. Example: file:///mydir/adviews.out.%(operatorId).part-%(partIndex)
   * 
   * @param filePathPattern
   *          The pattern of the output file
   */
  public void setFilePathPattern(String filePathPattern)
  {
    this.filePathPattern = filePathPattern;
  }

  /**
   * This returns the pattern of the output file
   * 
   * @return
   */
  public String getFilePathPattern()
  {
    return this.filePathPattern;
  }

  /**
   * Append to existing file. Default is true.
   * 
   * @param append
   *          This specifies if there exists a file with same name, then should the operator append to the existing file
   */
  public void setAppend(boolean append)
  {
    this.append = append;
  }

  /**
   * Bytes are written to the underlying file stream once they cross this size.<br>
   * Use this parameter if the file system used does not provide sufficient buffering. HDFS does buffering (even though
   * another layer of buffering on top appears to help) but other file system abstractions may not. <br>
   * 
   * @param bufferSize
   */
  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }

  /**
   * Replication factor. Value <= 0 indicates that the file systems default replication setting is used.
   * 
   * @param replication
   */
  public void setReplication(int replication)
  {
    this.replication = replication;
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

  public long getTotalBytesWritten()
  {
    return totalBytesWritten;
  }

  /**
   * @return the closeCurrentFile
   */
  public boolean isCloseCurrentFile()
  {
    return closeCurrentFile;
  }

  /**
   * @param closeCurrentFile
   *          the closeCurrentFile to set
   */
  public void setCloseCurrentFile(boolean closeCurrentFile)
  {
    this.closeCurrentFile = closeCurrentFile;
  }

  /**
   * This function opens the stream to given path.
   * 
   * @param filepath
   * @throws IOException
   */
  protected void openFile(Path filepath) throws IOException
  {
    if (replication <= 0) {
      replication = fs.getDefaultReplication(filepath);
    }
    if (fs.exists(filepath)) {
      if (append) {
        fsOutput = fs.append(filepath);
        logger.debug("appending to {}", filepath);
      } else {
        fs.delete(filepath, true);
        fsOutput = fs.create(filepath, (short) replication);
        logger.debug("creating {} with replication {}", filepath, replication);
      }
    } else {
      fsOutput = fs.create(filepath, (short) replication);
      logger.debug("creating {} with replication {}", filepath, replication);
    }
    if (bufferSize > 0) {
      this.bufferedOutput = new BufferedOutputStream(fsOutput, bufferSize);
      logger.debug("buffering with size {}", bufferSize);
    }

  }

  protected void closeFile() throws IOException
  {
    if (bufferedOutput != null) {
      bufferedOutput.close();
      bufferedOutput = null;
    }
    if (fsOutput != null) {
      fsOutput.close();
      fsOutput = null;
    }
  }

  /**
   * 
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    try {
      fs = FileSystem.get(new Configuration());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      closeFile();
    } catch (IOException ex) {
      throw new RuntimeException("Failed to close file. ", ex);
    }
    fs = null;
    filePathPattern = null;
    append = false;
  }

  @Override
  public void endWindow()
  {
    try {
      if (closeCurrentFile) {
        closeFile();
      } else {
        if (bufferedOutput != null) {
          bufferedOutput.flush();
        }
        fsOutput.hflush();
      }
    } catch (IOException ex) {
      throw new RuntimeException("Failed to flush.", ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (fsOutput == null) {
      try {
        validateNextFilePath();
        openFile(currentFilePath);
      } catch (IOException e) {
        throw new RuntimeException("Failed to open the file.", e);
      }
      currentBytesWritten = 0;
    }
  }

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>() {
    @Override
    public void process(T t)
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
        } else {
          fsOutput.write(tupleBytes);
        }
        currentBytesWritten += tupleBytes.length;
        totalBytesWritten += tupleBytes.length;
      } catch (IOException ex) {
        throw new RuntimeException("Failed to write to stream.", ex);
      }
    }
  };

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

  /**
   * This function returns the byte array for the given tuple.
   * 
   * @param t
   * @return
   */
  public abstract byte[] getBytesForTuple(T t);
}
