/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.ActivationListener;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.InputOperator;
import java.io.IOException;
import java.io.InputStream;

import javax.validation.constraints.NotNull;

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
 * Input Adapter for reading from File<p>
 * <br>
 *
 * <br>
 *
 * @param <STREAM>
 */
public abstract class AbstractFileInputOperator<STREAM extends InputStream> implements InputOperator, ActivationListener<Context>
{
  protected transient STREAM input;
  private long filepos;
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
  public void activate(Context ctx)
  {
    input = openFile(filePath);
    seek(input, filepos);
  }

  @Override
  public void deactivate()
  {
    try {
      input.close();
      input = null;
    }
    catch (IOException ex) {
      logger.error("exception on close", ex);
    }
  }

  @Override
  public void emitTuples()
  {
    emitTuples(input);
  }

  /**
   *
   * @param stream
   */
  public abstract void emitTuples(STREAM stream);

  @Override
  public void endWindow()
  {
    filepos = getFilePointer(input);
  }

  /**
   * Create an input stream for the path specified.
   *
   * @param filePath - path of the file which needs to be read.
   * @return - input stream created.
   */
  public abstract STREAM openFile(String filePath);

  /**
   * Returns the current offset in this file.
   *
   * @param stream
   * @return the offset from the beginning of the file, in bytes, at which the next read or write occurs.
   */
  public abstract long getFilePointer(STREAM stream);

  /**
   * Sets the file-pointer offset, measured from the beginning of this file, at which the next read or write occurs.
   * The offset may be set beyond the end of the file. Setting the offset beyond the end of the file does not change
   * the file length. The file length will change only by writing after the offset has been set beyond the end of the file.
   *
   * @param stream
   * @param pos the offset position, measured in bytes from the beginning of the file, at which to set the file pointer.
   */
  public abstract void seek(STREAM stream, long pos);

  private static final Logger logger = LoggerFactory.getLogger(AbstractFileInputOperator.class);
}
