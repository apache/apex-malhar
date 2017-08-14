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
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry;

import com.datatorrent.netlet.util.Slice;

/**
 * TFileReader
 *
 * @since 2.0.0
 */
@InterfaceStability.Evolving
public class TFileReader implements FileAccess.FileReader
{

  private final Reader reader;
  private final Scanner scanner;
  private final FSDataInputStream fsdis;
  private boolean closed = false;

  public TFileReader(FSDataInputStream fsdis, long fileLength, Configuration conf) throws IOException
  {
    this.fsdis = fsdis;
    reader = new Reader(fsdis, fileLength, conf);
    scanner = reader.createScanner();
  }

  /**
   * Unlike the TFile.Reader.close method this will close the wrapped InputStream.
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException
  {
    closed = true;
    scanner.close();
    reader.close();
    fsdis.close();
  }

  @Override
  public void readFully(TreeMap<Slice, Slice> data) throws IOException
  {
    scanner.rewind();
    for (; !scanner.atEnd(); scanner.advance()) {
      Entry en = scanner.entry();
      int klen = en.getKeyLength();
      int vlen = en.getValueLength();
      byte[] key = new byte[klen];
      byte[] value = new byte[vlen];
      en.getKey(key);
      en.getValue(value);
      data.put(new Slice(key, 0, key.length), new Slice(value, 0, value.length));
    }

  }

  @Override
  public void reset() throws IOException
  {
    scanner.rewind();
  }

  @Override
  public boolean seek(Slice key) throws IOException
  {
    try {
      return scanner.seekTo(key.buffer, key.offset, key.length);
    } catch (NullPointerException ex) {
      if (closed) {
        throw new IOException("Stream was closed");
      } else {
        throw ex;
      }
    }
  }

  @Override
  public boolean peek(Slice key, Slice value) throws IOException
  {
    if (scanner.atEnd()) {
      return false;
    }
    Entry en = scanner.entry();
    byte[] rkey = new byte[en.getKeyLength()];
    byte[] rval = new byte[en.getValueLength()];
    en.getKey(rkey);
    en.getValue(rval);

    key.buffer = rkey;
    key.offset = 0;
    key.length = en.getKeyLength();

    value.buffer = rval;
    value.offset = 0;
    value.length = en.getValueLength();

    return true;
  }

  @Override
  public boolean next(Slice key, Slice value) throws IOException
  {
    if (peek(key, value)) {
      scanner.advance();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean hasNext()
  {
    return !scanner.atEnd();
  }
}


