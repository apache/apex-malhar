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

import java.io.FileInputStream;
import java.io.IOException;

import com.datatorrent.api.Context.OperatorContext;

/**
 * This is an abstract input operator, which reads tuples from a file on the local file system.
 *
 * <br>
 * Extends AbstractInputAdapter<br>
 * Users need to implement getRecord to get HDFS input adapter to work as per their choice<br>
 * <br>
 *
 * @displayName Local FS File Input
 * @category io
 * @tags local fs, file, input
 *
 * @since 0.3.2
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
}
