/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.deprecated.api.SyncInputOperator;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
/**
 * Input Adapter for reading from HDFS<p>
 * <br>
 * Extends AbstractInputAdapter<br>
 * Users need to implement getRecord to get HDFS input adapter to work as per their choice<br>
 * <br>
 */
public abstract class AbstractHDFSInputOperator extends BaseOperator implements SyncInputOperator, Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractHDFSInputOperator.class);
  protected FSDataInputStream input;
  private boolean skipEndStream = false;
  private FileSystem fs;
  private Path filepath;

  protected abstract void emitRecord(FSDataInputStream input);

  protected abstract void emitEndStream();

  /**
   *
   * @return boolean
   */
  public boolean isSkipEndStream()
  {
    return skipEndStream;
  }

  @Override
  public Runnable getDataPoller()
  {
    return this;
  }

  @Override
  public void beginWindow()
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorConfiguration config)
  {
    try {
      fs = FileSystem.get(config);
      filepath = new Path(config.get("filepath"));
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    try {
      input = fs.open(filepath);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      input.close();
      input = null;
    }
    catch (IOException ex) {
      logger.error(ex.getLocalizedMessage());
    }
    fs = null;
    filepath = null;
  }

  @Override
  public void run()
  {
    logger.debug("ready to read hdfs file");
    try {
      while (true) {
        emitRecord(input);
      }
    }
    catch (Exception e) {
      logger.info("Exception on HDFS Input: {}", e.getLocalizedMessage());
      if (skipEndStream) {
        logger.info("Skipping end stream as requested");
      }
      else {
        logger.info("Ending the stream");
        emitEndStream();
      }
    }
  }

  public void setSkipEndStream(boolean skip)
  {
    this.skipEndStream = skip;
  }
}
