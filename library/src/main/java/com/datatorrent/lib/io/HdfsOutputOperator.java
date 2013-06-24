/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.Context.OperatorContext;

/**
 * Adapter for writing to HDFS<p>
 * <br>
 * Serializes tuples into a HDFS file<br>
 * Tuples are written to a single HDFS file, with the option to specify
 * size based file rolling, using place holders in the file path.<br>
 * Future enhancements may include options to write into a time slot/windows based files<br>
 * <br>
 */
public class HdfsOutputOperator extends BaseOperator
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(HdfsOutputOperator.class);
  private transient FSDataOutputStream fsOutput;
  private transient BufferedOutputStream bufferedOutput;
  private transient FileSystem fs;
  // internal persistent state
  private int fileIndex = 0;
  private int currentBytesWritten = 0;
  private long totalBytesWritten = 0;
  /**
   * File name substitution parameter: The system assigned id of the operator
   * instance, which is unique for the application.
   */
  public static final String FNAME_SUB_CONTEXT_ID = "contextId";
  /**
   * File name substitution parameter: The logical id assigned to the operator when assembling the DAG.
   */
  public static final String FNAME_SUB_OPERATOR_ID = "operatorId";
  /**
   * File name substitution parameter: Index of part file when a file size limit is specified.
   */
  public static final String FNAME_SUB_PART_INDEX = "partIndex";
  private int contextId;
  private String filePath;
  private boolean append = true;
  private int bufferSize = 0;
  private int bytesPerFile = 0;
  private int replication = 0;

  /**
   * The file name. This can be a relative path for the default file system
   * or fully qualified URL as accepted by ({@link org.apache.hadoop.fs.Path}).
   * For splits with per file size limit, the name needs to
   * contain substitution tokens to generate unique file names.
   * Example: file:///mydir/adviews.out.%(operatorId).part-%(partIndex)
   */
  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  /**
   * Append to existing file. Default is true.
   */
  public void setAppend(boolean append)
  {
    this.append = append;
  }

  /**
   * Bytes are written to the underlying file stream once they cross this size.<br>
   * Use this parameter if the file system used does not provide sufficient buffering.
   * HDFS does buffering (even though another layer of buffering on top appears to help)
   * but other file system abstractions may not.
   * <br>
   */
  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }

  /**
   * Replication factor. Value <= 0 indicates that the file systems default
   * replication setting is used.
   */
  public void setReplication(int replication)
  {
    this.replication = replication;
  }

  /**
   * Byte limit for a single file. Once the size is reached, a new file will be created.
   */
  public void setBytesPerFile(int bytesPerFile)
  {
    this.bytesPerFile = bytesPerFile;
  }

  public long getTotalBytesWritten()
  {
    return totalBytesWritten;
  }

  private Path subFilePath(int index)
  {
    Map<String, String> params = new HashMap<String, String>();
    params.put(FNAME_SUB_PART_INDEX, String.valueOf(index));
    params.put(FNAME_SUB_CONTEXT_ID, Integer.toString(contextId));
    params.put(FNAME_SUB_OPERATOR_ID, this.getName());
    StrSubstitutor sub = new StrSubstitutor(params, "%(", ")");
    return new Path(sub.replace(filePath.toString()));
  }

  private void openFile(Path filepath) throws IOException
  {
    if (fs.exists(filepath)) {
      if (append) {
        fsOutput = fs.append(filepath);
        logger.info("appending to {}", filepath);
      }
      else {
        fs.delete(filepath, true);
        if (replication <= 0) {
          replication = fs.getDefaultReplication(filepath);
        }
        fsOutput = fs.create(filepath, (short)replication);
        logger.info("creating {} with replication {}", filepath, replication);
      }
    }
    else {
      fsOutput = fs.create(filepath);
    }

    if (bufferSize > 0) {
      this.bufferedOutput = new BufferedOutputStream(fsOutput, bufferSize);
      logger.info("buffering with size {}", bufferSize);
    }

  }

  private void closeFile() throws IOException
  {
    if (bufferedOutput != null) {
      bufferedOutput.close();
      bufferedOutput = null;
    }
    fsOutput.close();
    fsOutput = null;
  }

  /**
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    this.contextId = context.getId();
    try {
      Path filepath = subFilePath(this.fileIndex);
      fs = FileSystem.get(filepath.toUri(), new Configuration());

      if (bytesPerFile > 0) {
        // ensure file path generates unique names
        Path p1 = subFilePath(1);
        Path p2 = subFilePath(2);
        if (p1.equals(p2)) {
          throw new IllegalArgumentException("Rolling files require %() placeholders for unique names: " + filepath);
        }
      }
      openFile(filepath);
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
    }
    catch (IOException ex) {
      logger.info("", ex);
    }

    fs = null;
    filePath = null;
    append = false;
  }

  @Override
  public void endWindow() {
    try {
      if (bufferedOutput != null) {
        bufferedOutput.flush();
      } else {
        fsOutput.flush();
      }
    } catch (IOException ex) {
      throw new RuntimeException("Failed to flush", ex);
    }
  }

  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object t)
    {
      // writing directly to the stream, assuming that HDFS already buffers block size.
      // check whether writing to separate in memory byte stream would be faster
      byte[] tupleBytes = t.toString().concat("\n").getBytes();
      try {
        if (bytesPerFile > 0 && currentBytesWritten + tupleBytes.length > bytesPerFile) {
          closeFile();
          Path filepath = subFilePath(++fileIndex);
          openFile(filepath);
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
        logger.error("Failed to write to stream.", ex);
      }
    }
  };
}
