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
package org.apache.apex.malhar.lib.fileaccess;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.file.tfile.TFile.Writer;

import com.datatorrent.netlet.util.Slice;

/**
 * TFileWriter
 *
 * @since 2.0.0
 */
@InterfaceStability.Evolving
public final class TFileWriter implements FileAccess.FileWriter
{
  private Writer writer;

  private FSDataOutputStream fsdos;

  public TFileWriter(FSDataOutputStream stream, int minBlockSize, String compressName,
      String comparator, Configuration conf) throws IOException
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
  public void append(Slice key, Slice value) throws IOException
  {
    writer.append(key.buffer, key.offset, key.length, value.buffer, value.offset, value.length);
  }

  @Override
  public long getBytesWritten() throws IOException
  {
    return fsdos.getPos();
  }

}
