/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.io.BufferedOutputStream;
import java.io.IOException;

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
 * Currently all tuples are written to a single HDFS file<br>
 * Future enhancements include options to write into a time slot/windows based files<br>
 * <br>
 *
 */
@ModuleAnnotation(ports= {
  @PortAnnotation(name = Component.INPUT, type = PortAnnotation.PortType.INPUT)
})
public class HdfsOutputModule extends AbstractModule
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(HdfsOutputModule.class);
  private FSDataOutputStream fsOutput;
  private BufferedOutputStream bufferedOutput;
  private SerDe serde; // it was taken from context before, but now what, it's not a stream but a node!
  private FileSystem fs;
  private Path filepath;
  private boolean append;
  private int bufferSize;
  private int replication;

  int bytesWritten = 0;

  public static final String KEY_FILEPATH = "filepath";
  public static final String KEY_APPEND = "append";
  public static final String KEY_REPLICATION = "replication";

  /**
   * Bytes are written to the underlying file stream once they cross this size.<br>
   * Use this parameter only if the file system used does not already buffer.
   * HDFS does buffering but other file system abstractions may not.
   * <br>
   */
  public static final String KEY_BUFFERSIZE = "bufferSize";

  /**
   *
   * @param config
   */
  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    try {
      filepath = new Path(config.get(KEY_FILEPATH));
      fs = FileSystem.get(filepath.toUri(), config);
      append = config.getBoolean(KEY_APPEND, true);
      replication = config.getInt(KEY_REPLICATION, 0);

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

      this.bufferSize = config.getInt(KEY_BUFFERSIZE, 0);
      if (bufferSize > 0) {
        this.bufferedOutput = new BufferedOutputStream(fsOutput, bufferSize);
        logger.info("buffering with size {}", bufferSize);
      }

    }
    catch (IOException ex) {
      throw new FailedOperationException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      if (bufferedOutput != null) {
        bufferedOutput.close();
        bufferedOutput = null;
      }
      fsOutput.close();
      fsOutput = null;
    }
    catch (IOException ex) {
      logger.info("", ex);
    }

    serde = null;
    fs = null;
    filepath = null;
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
        if (bufferedOutput != null) {
          bufferedOutput.write(tupleBytes);
        } else {
          fsOutput.write(tupleBytes);
        }
        bytesWritten += tupleBytes.length;
      } catch (IOException ex) {
        logger.error("Failed to write to stream.", ex);
      }
    }
  }

}
