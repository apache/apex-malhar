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
package com.datatorrent.contrib.hds.tfile;

import java.io.IOException;
import java.util.Arrays;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry;

import com.datatorrent.contrib.hds.HDSFileAccess.HDSFileReader;
import com.datatorrent.contrib.hds.MutableKeyValue;

public class TFileReader implements HDSFileReader
{

  private Reader reader;
  private Scanner scanner;
  private FSDataInputStream fsdis;

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
    scanner.close();
    reader.close();
    fsdis.close();
  }

  @Override
  public void readFully(TreeMap<byte[], byte[]> data) throws IOException
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
      data.put(key, value);
    }

  }

  @Override
  public byte[] getValue(byte[] key) throws IOException
  {
    seek(key);
    Entry en = scanner.entry();
    byte[] rkey = new byte[en.getKeyLength()];
    byte[] rval = new byte[en.getValueLength()];
    en.getKey(rkey);
    // If the key doesn't exist
    if (!Arrays.equals(key, rkey)) {
      return null;
    } else {
      en.getValue(rval);
      return rval;
    }
  }

  @Override
  public void reset() throws IOException
  {
    scanner.rewind();
  }

  @Override
  public void seek(byte[] key) throws IOException
  {
    if (!scanner.seekTo(key)) {
      throw new IOException("The key is not seekable");
    }
  }

  @Override
  public boolean next() throws IOException
  {
    return scanner.advance();
  }

  @Override
  public void get(MutableKeyValue mutableKeyValue) throws IOException
  {
    Entry en = scanner.entry();
    byte[] rkey = new byte[en.getKeyLength()];
    byte[] rval = new byte[en.getValueLength()];
    en.getKey(rkey);
    en.getValue(rval);
    mutableKeyValue.setKey(rkey);
    mutableKeyValue.setValue(rval);
  }

}
