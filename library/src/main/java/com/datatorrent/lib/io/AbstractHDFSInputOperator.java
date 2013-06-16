/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io;

import com.malhartech.api.Context.OperatorContext;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Input Adapter for reading from HDFS File
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractHDFSInputOperator extends AbstractFileInputOperator<FSDataInputStream>
{
  @Override
  public FSDataInputStream openFile(String filePath)
  {
    try {
      fs = FileSystem.get(new Configuration());
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    try {
      return fs.open(new Path(filePath));
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
  public long getFilePointer(FSDataInputStream stream)
  {
    try {
      return stream.getPos();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex.getCause());
    }
  }

  @Override
  public void seek(FSDataInputStream stream, long pos)
  {
    try {
      stream.seek(pos);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex.getCause());
    }
  }

  private transient FileSystem fs;
  private static final Logger logger = LoggerFactory.getLogger(AbstractHDFSInputOperator.class);
}
