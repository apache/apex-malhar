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
  private String filePath;

  /**
   * The file name. This can be a relative path for the default file system
   * or fully qualified URL as accepted by ({@link org.apache.hadoop.fs.Path}).
   * For splits with per file size limit, the name needs to
   * contain substitution tokens to generate unique file names.
   * Example: file:///mydir/adviews.out.%(operatorId).part-%(partIndex)
   * @param filePath 
   */
  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

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
      input = fs.open(new Path(filePath));
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
    setFilePath(null);
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

}
