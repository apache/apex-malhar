/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.dag.SerDe;
import org.apache.hadoop.conf.Configuration;

/**
 * Adapter for writing to HDFS<p>
 * <br>
 * Serializes tuples into a HDFS file<br>
 * Tuples are written to a single HDFS file, with the option to specify
 * size based file rolling, using place holders in the file path.<br>
 * Future enhancements may include options to write into a time slot/windows based files<br>
 * <br>
 */
public class HdfsOutputOperator<T> extends BaseOperator
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(HdfsOutputOperator.class);
  private transient FSDataOutputStream fsOutput;
  private transient BufferedOutputStream bufferedOutput;
  private SerDe serde; // it was taken from context before, but now what, it's not a stream but a node!
  private transient FileSystem fs;

  // internal persistent state
  private int fileIndex = 0;
  private int currentBytesWritten = 0;
  private long totalBytesWritten = 0;

  public static final String FNAME_SUB_OPERATOR_ID = "operatorId";
  public static final String FNAME_SUB_PART_INDEX = "partIndex";

  /**
   * The file name. This can be a relative path for the default file system
   * or fully qualified URL as accepted by ({@link org.apache.hadoop.fs.Path}).
   * For splits with per file size limit, the name needs to
   * contain substitution tokens to generate unique file names.
   * Example: file:///mydir/adviews.out.%(operatorId).part-%(partIndex)
   */
  public String filePath;

  /**
   * Append to existing file. Default is true.
   */
  public boolean append = true;

  /**
   * Bytes are written to the underlying file stream once they cross this size.<br>
   * Use this parameter if the file system used does not provide sufficient buffering.
   * HDFS does buffering (even though another layer of buffering on top appears to help)
   * but other file system abstractions may not.
   * <br>
   */
  public int bufferSize = 0;

  /**
   * Replication factor. Value <= 0 indicates that the file systems default
   * replication setting is used.
   */
  private int replication = 0;

  /**
   * Byte limit for a single file. Once the size is reached, a new file will be created.
   */
  public int bytesPerFile = 0;

  public long getTotalBytesWritten()
  {
    return totalBytesWritten;
  }

  private Path subFilePath(int index)
  {
    Map<String, String> params = new HashMap<String, String>();
    params.put(FNAME_SUB_PART_INDEX, String.valueOf(index));
    // TODO: need access to the operator id
    String operatorId = this.getName();
    if (operatorId != null) {
      params.put(FNAME_SUB_OPERATOR_ID, operatorId.replace(":", ""));
    }
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
   * @param config
   */
  @Override
  public void setup(OperatorContext context)
  {
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

    serde = null;
    fs = null;
    filePath = null;
    append = false;
  }

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T t)
    {
      // writing directly to the stream, assuming that HDFS already buffers block size.
      // check whether writing to separate in memory byte stream would be faster
      byte[] tupleBytes;
      if (serde == null) {
        tupleBytes = t.toString().concat("\n").getBytes();
      }
      else {
        tupleBytes = serde.toByteArray(t);
      }
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
