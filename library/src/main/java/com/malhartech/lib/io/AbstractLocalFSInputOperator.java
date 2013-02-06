/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.InputOperator;
import java.io.FileInputStream;
import java.io.IOException;
import javax.validation.constraints.NotNull;
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
public abstract class AbstractLocalFSInputOperator extends BaseOperator implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractLocalFSInputOperator.class);
  protected transient FileInputStream input;
  @NotNull
  private String filePath;

  public String getFilePath()
  {
    return filePath;
  }

  /**
   * The file name. This can be a relative path for the default file system
   * or fully qualified URL as accepted by ({@link org.apache.hadoop.fs.Path}).
   * For splits with per file size limit, the name needs to
   * contain substitution tokens to generate unique file names.
   * Example: file:///mydir/adviews.out.%(operatorId).part-%(partIndex)
   *
   * @param filePath
   */
  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

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
      input = new FileInputStream(filePath);
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
      logger.error("exception on close", ex);
    }
  }

}
