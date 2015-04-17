/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io.block;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;

/**
 * An {@link AbstractBlockReader} that assumes the blocks are part of files.
 *
 * @param <R> type of record
 */
public abstract class AbstractFSBlockReader<R> extends AbstractBlockReader<R, BlockMetadata.FileBlockMetadata, FSDataInputStream>
{
  protected transient FileSystem fs;
  protected transient Configuration configuration;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    configuration = new Configuration();
    try {
      fs = getFSInstance();
    }
    catch (IOException e) {
      throw new RuntimeException("creating fs", e);
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    try {
      fs.close();
    }
    catch (IOException e) {
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
    return FileSystem.newInstance(configuration);
  }

  /**
   * An {@link AbstractFSBlockReader} which reads lines from the block using {@link ReaderContext.LineReaderContext}
   *
   * @param <R> type of records
   */
  public static abstract class AbstractFSLineReader<R> extends AbstractFSBlockReader<R>
  {

    public AbstractFSLineReader()
    {
      super();
      this.readerContext = new ReaderContext.LineReaderContext<FSDataInputStream>();
    }
  }

  /**
   * An {@link AbstractFSBlockReader} which reads lines from the block using {@link ReaderContext.ReadAheadLineReaderContext}
   *
   * @param <R>
   */
  public static abstract class AbstractFSReadAheadLineReader<R> extends AbstractFSBlockReader<R>
  {
    public AbstractFSReadAheadLineReader()
    {
      super();
      this.readerContext = new ReaderContext.ReadAheadLineReaderContext<FSDataInputStream>();
    }
  }
}
