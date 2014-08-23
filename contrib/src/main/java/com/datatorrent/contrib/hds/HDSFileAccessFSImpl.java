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
package com.datatorrent.contrib.hds;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.common.util.Pair;
import com.datatorrent.common.util.Slice;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Hadoop file system backed store.
 */
public class HDSFileAccessFSImpl implements HDSFileAccess
{
  @NotNull
  private String basePath;
  protected transient FileSystem fs;

  public HDSFileAccessFSImpl()
  {
  }

  public String getBasePath()
  {
    return basePath;
  }

  public void setBasePath(String path)
  {
    this.basePath = path;
  }

  protected Path getFilePath(long bucketKey, String fileName) {
    return new Path(getBucketPath(bucketKey), fileName);
  }

  protected Path getBucketPath(long bucketKey)
  {
    return new Path(basePath, Long.toString(bucketKey));
  }

  public long getFileSize(long bucketKey, String fileName) throws IOException {
    return fs.getFileStatus(getFilePath(bucketKey, fileName)).getLen();
  }

  @Override
  public void close() throws IOException
  {
    fs.close();
  }

  @Override
  public void init()
  {
    if (fs == null) {
      Path dataFilePath = new Path(basePath);
      try {
        fs = FileSystem.newInstance(dataFilePath.toUri(), new Configuration());
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void delete(long bucketKey, String fileName) throws IOException
  {
    fs.delete(getFilePath(bucketKey, fileName), true);
  }


  @Override
  public FSDataOutputStream getOutputStream(long bucketKey, String fileName) throws IOException
  {
    Path path = getFilePath(bucketKey, fileName);
    return fs.create(path, true);
  }

  @Override
  public FSDataInputStream getInputStream(long bucketKey, String fileName) throws IOException
  {
    return fs.open(getFilePath(bucketKey, fileName));
  }

  @Override
  public void rename(long bucketKey, String fromName, String toName) throws IOException
  {
    FileContext fc = FileContext.getFileContext(fs.getUri());
    Path bucketPath = getBucketPath(bucketKey);
    fc.rename(new Path(bucketPath, fromName), new Path(bucketPath, toName), Rename.OVERWRITE);
  }

  private final transient Kryo kryo = new Kryo();

  @Override
  public HDSFileReader getReader(final long bucketKey, final String fileName) throws IOException
  {
    final HashMap<Slice, Pair<byte[], Integer>> data = Maps.newHashMap();
    final ArrayList<Slice> keys = Lists.newArrayList();
    final MutableInt index = new MutableInt();

    DataInputStream is = getInputStream(bucketKey, fileName);
    Input input = new Input(is);
    while (!input.eof()) {
      byte[] keyBytes = kryo.readObject(input, byte[].class);
      byte[] value = kryo.readObject(input, byte[].class);
      Slice key = new Slice(keyBytes, 0, keyBytes.length);
      data.put(key, new Pair<byte[], Integer>(value, keys.size()));
      keys.add(key);
    }
    input.close();
    is.close();

    return new HDSFileReader() {

      @Override
      public void readFully(TreeMap<Slice, byte[]> result) throws IOException
      {
        for (Map.Entry<Slice, Pair<byte[], Integer>> e : data.entrySet()) {
          result.put(e.getKey(), e.getValue().first);
        }
      }

      @Override
      public void reset() throws IOException {
        index.setValue(0);
      }

      @Override
      public boolean seek(byte[] key) throws IOException {
        Pair<byte[], Integer> v = data.get(new Slice(key, 0, key.length));
        if (v == null) {
          index.setValue(0);
          return false;
        }
        index.setValue(v.second);
        return true;
      }

      @Override
      public boolean next(Slice key, Slice value) throws IOException {
        int pos = index.intValue();
        if (pos < keys.size()) {
          Slice k = keys.get(pos);
          key.buffer = k.buffer;
          key.offset = k.offset;
          key.length = k.length;
          Pair<byte[], Integer> v = data.get(k);
          value.buffer = v.first;
          value.offset = 0;
          value.length = v.first.length;
          index.increment();
        }
        return false;
      }

      @Override
      public void close() throws IOException {
      }
    };
  }

  @Override
  public HDSFileWriter getWriter(final long bucketKey, final String fileName) throws IOException
  {
    final DataOutputStream dos = getOutputStream(bucketKey, fileName);
    final CountingOutputStream cos = new CountingOutputStream(dos);
    final Output out = new Output(cos);

    return new HDSFileWriter() {
      @Override
      public void close() throws IOException
      {
        out.close();
        cos.close();
        dos.close();
      }

      @Override
      public void append(byte[] key, byte[] value) throws IOException
      {
        kryo.writeObject(out, key);
        kryo.writeObject(out, value);
      }

      @Override
      public long getBytesWritten()
      {
        return cos.getCount() + out.position();
      }

    };

  }

  @Override
  public String toString()
  {
    return "HDSFileAccessFSImpl [basePath=" + basePath + "]";
  }

}
