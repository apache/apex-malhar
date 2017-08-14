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
package org.apache.apex.malhar.lib.io.block;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.StatsListener;

/**
 * An {@link AbstractBlockReader} that assumes the blocks are part of files.
 *
 * @param <R> type of record
 *
 * @since 2.1.0
 */
@StatsListener.DataQueueSize
public abstract class AbstractFSBlockReader<R>
    extends AbstractBlockReader<R, BlockMetadata.FileBlockMetadata, FSDataInputStream>
{
  protected transient FileSystem fs;
  protected transient Configuration configuration;

  /**
   * If all the blocks belong to files which are under a folder than this base can be set to that folder.
   * The File System instance is derived using this base path.
   */
  protected String basePath;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    configuration = new Configuration();
    try {
      fs = getFSInstance();
    } catch (IOException e) {
      throw new RuntimeException("creating fs", e);
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
  protected FSDataInputStream setupStream(BlockMetadata.FileBlockMetadata block) throws IOException
  {
    return fs.open(new Path(block.getFilePath()));
  }

  /**
   * Override this method to change the FileSystem instance that is used by the operator.
   *
   * @return A FileSystem object.
   * @throws IOException
   */
  protected FileSystem getFSInstance() throws IOException
  {
    if (basePath != null) {
      return FileSystem.newInstance(URI.create(basePath), configuration);
    } else  {
      return FileSystem.newInstance(configuration);
    }
  }

  /**
   * An {@link AbstractFSBlockReader} which reads lines from the block using {@link ReaderContext.LineReaderContext}
   *
   * @param <R> type of records
   */
  public abstract static class AbstractFSLineReader<R> extends AbstractFSBlockReader<R>
  {

    public AbstractFSLineReader()
    {
      super();
      this.readerContext = new ReaderContext.LineReaderContext<>();
    }
  }

  /**
   * An {@link AbstractFSBlockReader} which reads lines from the block using
   * {@link ReaderContext.ReadAheadLineReaderContext}
   *
   * @param <R> type of record.
   */
  public abstract static class AbstractFSReadAheadLineReader<R> extends AbstractFSBlockReader<R>
  {
    public AbstractFSReadAheadLineReader()
    {
      super();
      this.readerContext = new ReaderContext.ReadAheadLineReaderContext<>();
    }
  }

  /**
   * Sets the base path.
   */
  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }

  public String getBasePath()
  {
    return basePath;
  }
}
