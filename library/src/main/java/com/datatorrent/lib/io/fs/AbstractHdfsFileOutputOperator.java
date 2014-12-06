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

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;

/**
 * This is the base implementation of an HDFS output operator, which writes tuples to an HDFS file.&nbsp;
 * Subclasses should implement the methods required to process and serialize the tuples.
 * <p>
 * Contains base implementations for setup, teardown, open file and close file.
 * </p>
 * @displayName HDFS File Output
 * @category Output
 * @tags hdfs, file, output operator
 *
 * @param <INPUT> incoming tuple type
 * @since 1.0.2
 * @deprecated This base implementation is not fault tolerant. Please extend {@link AbstractFSWriter} for output operators instead.
 */
@Deprecated
public abstract class AbstractHdfsFileOutputOperator<INPUT> extends BaseOperator
{
  protected transient FSDataOutputStream fsOutput;
  protected transient BufferedOutputStream bufferedOutput;
  protected transient FileSystem fs;
  @NotNull
  protected String filePath;
  protected long totalBytesWritten = 0;
  protected boolean append = true;
  protected int bufferSize = 0;
  protected int replication = 0;

  /**
   * This is the input port which receives tuples to be written out to HDFS.
   */
  public final transient DefaultInputPort<INPUT> input = new DefaultInputPort<INPUT>()
  {
    @Override
    public void process(INPUT t)
    {
      processTuple(t);
    }

  };

  /**
   * Function to be implemented to process each incoming tuple
   *
   * @param t incoming tuple
   */
  protected abstract void processTuple(INPUT t);

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
      }
      else {
        fs.delete(filepath, true);
        fsOutput = fs.create(filepath, (short)replication);
        logger.debug("creating {} with replication {}", filepath, replication);
      }
    }
    else {
      fsOutput = fs.create(filepath, (short)replication);
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
      fs = FileSystem.newInstance(new Configuration());
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      closeFile();
      if (fs != null) {
        fs.close();
      }
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    fs = null;
    append = false;
  }

  /**
   * The file name. This can be a relative path for the default file system or fully qualified URL as accepted by (
   * {@link org.apache.hadoop.fs.Path}). For splits with per file size limit, the name needs to contain substitution
   * tokens to generate unique file names. Example: file:///mydir/adviews.out.%(operatorId).part-%(partIndex)
   *
   * @param filePath
   * The pattern of the output file
   */
  public void setFilePath(@NotNull String filePath)
  {
    this.filePath = filePath;
  }

  /**
   * This returns the pattern of the output file
   *
   * @return The pattern of the output file.
   */
  public String getFilePath()
  {
    return this.filePath;
  }

  /**
   * Append to existing file. Default is true.
   *
   * @param append
   * This specifies if there exists a file with same name, then should the operator append to the existing file
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

  public long getTotalBytesWritten()
  {
    return totalBytesWritten;
  }

  /**
   * This function returns the byte array for the given tuple.
   * @param t The tuple to convert into a byte array.
   * @return The byte array for a given tuple.
   */
  protected abstract byte[] getBytesForTuple(INPUT t);

  private static final Logger logger = LoggerFactory.getLogger(AbstractHdfsFileOutputOperator.class);
}
