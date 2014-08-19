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
import java.util.TreeMap;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;

import com.datatorrent.common.util.DTThrowable;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

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

  protected Path getBucketPath(long bucketKey)
  {
    return new Path(basePath, Long.toString(bucketKey));
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
    fs.delete(new Path(getBucketPath(bucketKey), fileName), true);
  }

  @Override
  public FSDataOutputStream getOutputStream(long bucketKey, String fileName) throws IOException
  {
    Path path = new Path(getBucketPath(bucketKey), fileName);
    if (!fs.exists(path)) {
      return fs.create(path);
    }
    return fs.append(path);
  }

  @Override
  public FSDataInputStream getInputStream(long bucketKey, String fileName) throws IOException
  {
    return fs.open(new Path(getBucketPath(bucketKey), fileName));
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
    final DataInputStream is = getInputStream(bucketKey, fileName);
    return new HDSFileReader() {

      @Override
      public void readFully(TreeMap<byte[], byte[]> data) throws IOException
      {
        Input input = new Input(is);
        while (!input.eof()) {
          byte[] key = kryo.readObject(input, byte[].class);
          byte[] value = kryo.readObject(input, byte[].class);
          data.put(key, value);
        }
      }

      @Override
      public byte[] getValue(byte[] key) throws IOException {
        throw new UnsupportedOperationException("Operation not implemented");
      }

      @Override
      public void reset() throws IOException {
        is.reset();
      }

      @Override
      public void seek(byte[] key) throws IOException {
        throw new UnsupportedOperationException("Operation not implemented");
      }

      @Override
      public boolean next() throws IOException {
        throw new UnsupportedOperationException("Operation not implemented");
      }

      @Override
      public void get(MutableKeyValue mutableKeyValue) throws IOException {
        throw new UnsupportedOperationException("Operation not implemented");
      }

      @Override
      public void close() throws IOException {
        is.close();
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
      public int getFileSize()
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
