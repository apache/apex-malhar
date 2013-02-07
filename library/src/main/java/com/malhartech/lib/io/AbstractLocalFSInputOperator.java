/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.Context.OperatorContext;
import java.io.FileInputStream;
import java.io.IOException;
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
public abstract class AbstractLocalFSInputOperator extends AbstractFileInputOperator<FileInputStream>
{
  @Override
  public FileInputStream openFile(String filePath)
  {
    try {
      return new FileInputStream(filePath);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public long getFilePointer(FileInputStream stream)
  {
    try {
      return stream.getChannel().position();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void seek(FileInputStream stream, long pos)
  {
    try {
      stream.getChannel().position(pos);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractLocalFSInputOperator.class);
}
