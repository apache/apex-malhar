/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.io.fs;

import java.io.IOException;
import java.util.LinkedList;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;

/**
 * A file splitter that receives its input from an upstream operator.
 *
 * @since 3.2.0
 */
public class FileSplitterBase extends AbstractFileSplitter implements Operator.IdleTimeHandler
{
  @NotNull
  protected String file;
  protected transient FileSystem fs;

  protected final LinkedList<FileInfo> fileInfos;
  protected transient int sleepTimeMillis;

  public FileSplitterBase()
  {
    fileInfos = new LinkedList<>();
  }

  public final transient DefaultInputPort<FileInfo> input = new DefaultInputPort<FileInfo>()
  {
    @Override
    public void process(FileInfo fileInfo)
    {
      fileInfos.add(fileInfo);
      FileSplitterBase.this.process();
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    sleepTimeMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
    try {
      fs = getFSInstance();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    super.setup(context);
  }

  protected FileSystem getFSInstance() throws IOException
  {
    return FileSystem.newInstance(new Path(file).toUri(), new Configuration());
  }

  @Override
  protected FileInfo getFileInfo()
  {
    if (fileInfos.size() > 0) {
      return fileInfos.remove();
    }
    return null;
  }

  @Override
  public void handleIdleTime()
  {
    if (blockCount < blocksThreshold && (blockMetadataIterator != null || fileInfos.size() > 0)) {
      process();
    } else {
      /* nothing to do here, so sleep for a while to avoid busy loop */
      try {
        Thread.sleep(sleepTimeMillis);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    try {
      fs.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected long getDefaultBlockSize()
  {
    return fs.getDefaultBlockSize(new Path(file));
  }

  @Override
  protected FileStatus getFileStatus(Path path) throws IOException
  {
    return fs.getFileStatus(path);
  }

  /**
   * File path from which the File System is inferred.
   *
   * @param file files
   */
  public void setFile(@NotNull String file)
  {
    this.file = Preconditions.checkNotNull(file, "file path");
  }

  /**
   * @return file path from which the File System is inferred.
   */
  public String getFile()
  {
    return file;
  }
}
