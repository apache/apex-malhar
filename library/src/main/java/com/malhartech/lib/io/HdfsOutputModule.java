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

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.Component;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.SerDe;
import com.malhartech.dag.Tuple;

/**
 * Adapter for writing to HDFS<p>
 * <br>
 * Serializes tuples into a HDFS file<br>
 * Tuples are written to a single HDFS file, with the option to specify
 * size based file rolling, using place holders in the file path.<br>
 * Future enhancements may include options to write into a time slot/windows based files<br>
 * <br>
 */
@ModuleAnnotation(ports= {
  @PortAnnotation(name = Component.INPUT, type = PortAnnotation.PortType.INPUT)
})
public class HdfsOutputModule extends AbstractModule
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(HdfsOutputModule.class);
  private transient FSDataOutputStream fsOutput;
  private transient BufferedOutputStream bufferedOutput;
  private SerDe serde; // it was taken from context before, but now what, it's not a stream but a node!
  private transient FileSystem fs;
  private String filePathTemplate;
  private boolean append;
  private int bufferSize;
  private int replication;
  private int bytesPerFile;
  private int fileIndex = 0;

  int currentBytesWritten = 0;
  int totalBytesWritten = 0;

  public static final String KEY_FILEPATH = "filepath";
  public static final String KEY_APPEND = "append";
  public static final String KEY_REPLICATION = "replication";

  /**
   * Byte limit for a single file. Once the size is reached, a new file will be created.
   */
  public static final String KEY_BYTES_PER_FILE = "bytesPerFile";

  /**
   * Bytes are written to the underlying file stream once they cross this size.<br>
   * Use this parameter if the file system used does not provide sufficient buffering.
   * HDFS does buffering (even though another layer of buffering on top appears to help)
   * but other file system abstractions may not.
   * <br>
   */
  public static final String KEY_BUFFERSIZE = "bufferSize";


  public static final String FNAME_SUB_MODULE_ID = "moduleId";
  public static final String FNAME_SUB_PART_INDEX = "partIndex";

  private Path subFilePath(int index) {
    Map<String, String> params = new HashMap<String, String>();
    params.put(FNAME_SUB_PART_INDEX, String.valueOf(index));
    String moduleId  = this.getId();
    if (moduleId != null) {
      params.put(FNAME_SUB_MODULE_ID, moduleId.replace(":", ""));
    }
    StrSubstitutor sub = new StrSubstitutor(params, "%(", ")");
    return new Path(sub.replace(filePathTemplate.toString()));
  }

  private void openFile(Path filepath) throws IOException {
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

  private void closeFile() throws IOException {
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
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    try {
      filePathTemplate = config.get(KEY_FILEPATH);
      Path filepath = subFilePath(this.fileIndex);
      fs = FileSystem.get(filepath.toUri(), config);
      append = config.getBoolean(KEY_APPEND, true);
      replication = config.getInt(KEY_REPLICATION, 0);
      bytesPerFile = config.getInt(KEY_BYTES_PER_FILE, 0);
      bufferSize = config.getInt(KEY_BUFFERSIZE, 0);

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
      throw new FailedOperationException(ex);
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
    filePathTemplate = null;
    append = false;
  }


  /**
   *
   * @param t the value of t
   */
  @Override
  public void process(Object t) {
    if (t instanceof Tuple) {
      // TODO: is this still needed?
      logger.error("ignoring tuple " + t);
    }
    else {
      // writing directly to the stream, assuming that HDFS already buffers block size.
      // check whether writing to separate in memory byte stream would be faster
      byte[] tupleBytes;
      if (serde == null) {
        tupleBytes = t.toString().concat("\n").getBytes();
      } else {
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
        } else {
          fsOutput.write(tupleBytes);
        }

        currentBytesWritten += tupleBytes.length;
        totalBytesWritten += tupleBytes.length;

      } catch (IOException ex) {
        logger.error("Failed to write to stream.", ex);
      }
    }
  }

}
