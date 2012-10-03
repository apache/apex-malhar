/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.io.IOException;
import java.util.ArrayList;

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
  private FSDataOutputStream output;
  private SerDe serde; // it was taken from context before, but now what, it's not a stream but a node!
  private FileSystem fs;
  private Path filepath;
  private boolean append;
  private ArrayList alist = null;

  int byte_tuple_size_compute_count = 10;
  int byte_tuple_size_compute_size = 0;
  int byte_tuple_size = 0;
  int byte_flush_size = 0;

  int bytesWritten = 0;

  public static final String KEY_FILEPATH = "filepath";
  public static final String KEY_APPEND = "append";

/**
   * Bytes are written to the file once they cross this size. If end of window is reached the all bytes are written out anyway<br>
   * <br>
   */
  public static final String KEY_BYTE_FLUSH_SIZE = "byte_flush_size";

  /**
   * Averate tuple size. Used to compute the current buffer to flush<br>
   * <br>
   */
  public static final String KEY_BYTE_TUPLE_SIZE = "byte_tuple_size";

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

      if (fs.exists(filepath)) {
        if (append) {
          output = fs.append(filepath);
        }
        else {
          fs.delete(filepath, true);
          output = fs.create(filepath);
        }
      }
      else {
        output = fs.create(filepath);
      }
    }
    catch (IOException ex) {
      logger.debug(ex.getLocalizedMessage());
      throw new FailedOperationException(ex);
    }
    alist = new ArrayList(1000);
  }

  @Override
  public void teardown()
  {
    flushBytes();
    try {
      output.close();
      output = null;
    }
    catch (IOException ex) {
      logger.info("", ex);
    }

    serde = null;

    fs = null;
    filepath = null;
    append = false;
  }


  public void flushBytes() {
    if (!alist.isEmpty()) {
      byte[] serialized = serde.toByteArray(alist);
      try {
        output.write(serialized);
      }
      catch (IOException ex) {
        logger.info("", ex);
      }
      alist.clear();
    }
  }

  /**
   *
   * @param t the value of t
   */
  @Override
  public void process(Object t) {
    if (t instanceof Tuple) {
      logger.error("ignoring tuple " + t);
    }
    else {
      if (this.byte_flush_size == 0) {
        // writing directly to the stream, assuming that HDFS already buffers block size.
        // check whether writing to separate in memory byte stream would be faster
        byte[] tupleBytes;
        if (serde == null) {
          tupleBytes = t.toString().concat("\n").getBytes();
        } else {
          tupleBytes = serde.toByteArray(t);
        }
        try {
          output.write(tupleBytes);
          bytesWritten += tupleBytes.length;
        } catch (IOException ex) {
          logger.error("Failed to write to stream.", ex);
        }
      } else {
        alist.add(t);
        if (byte_tuple_size == 0) {
          if (byte_tuple_size_compute_count <= 0) {
            byte_tuple_size = byte_tuple_size_compute_size/10; // take average of 10 tuples
          }
          else {
            byte[] dump = serde.toByteArray(t);
            byte_tuple_size_compute_size += dump.length;
            byte_tuple_size_compute_count--;
          }
        }
        if (byte_flush_size != 0) { // do not wait till end of window to flush
          if ((alist.size() * byte_tuple_size) > byte_flush_size) {
            flushBytes();
          }
        }
      }
    }
  }
    @Override
  public void endWindow() {
      //flushBytes();
  }

}
