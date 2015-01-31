/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hdht.tfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.file.tfile.TFile.Writer;

import com.datatorrent.contrib.hdht.HDHTFileAccess.HDSFileWriter;

/**
 * TFileWriter
 *
 * @since 2.0.0
 */
public final class TFileWriter implements HDSFileWriter
{
  private Writer writer;
  
  private FSDataOutputStream fsdos;
  
  public TFileWriter(FSDataOutputStream stream, int minBlockSize, String compressName, String comparator, Configuration conf) throws IOException
  {
    this.fsdos = stream;
    writer = new Writer(stream, minBlockSize, compressName, comparator, conf);
    
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
    fsdos.close();
  }

  @Override
  public void append(byte[] key, byte[] value) throws IOException
  {
    writer.append(key, value);
  }

  @Override
  public long getBytesWritten() throws IOException{ return fsdos.getPos(); }

}
