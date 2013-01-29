/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.InputOperator;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class AbstractHDFSInputOperator extends BaseOperator implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractHDFSInputOperator.class);
  protected transient FSDataInputStream input;
  private transient FileSystem fs;
  private Path filepath;

  protected abstract void emitRecord(FSDataInputStream input);

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      fs = FileSystem.get(new Configuration());
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
    setFilepath(null);
  }

  @Override
  public void emitTuples()
  {
    try {
      emitRecord(input);
    }
    catch (Exception e) {
      logger.info("Exception on HDFS Input: {}", e.getLocalizedMessage());
      Thread.currentThread().interrupt();
    }
  }

  /**
   * @param filepath the filepath to set
   */
  public void setFilepath(Path filepath)
  {
    this.filepath = filepath;
  }

}
