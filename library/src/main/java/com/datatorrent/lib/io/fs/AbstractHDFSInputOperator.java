/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;

/**
 * This is an abstract input operator, which reads from a hdfs file and produces tuples.
 * <p></p>
 * @displayName HDFS File Input
 * @category io
 * @tags hdfs, file, input operator
 *
 * @since 0.3.2
 */
public abstract class AbstractHDFSInputOperator extends AbstractFileInputOperator<FSDataInputStream>
{
  @Override
  public FSDataInputStream openFile(String filePath)
  {
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
    try {
      fs = FileSystem.newInstance(new Configuration());
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    if(fs != null){
      try {
        fs.close();
	fs = null;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
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
}
