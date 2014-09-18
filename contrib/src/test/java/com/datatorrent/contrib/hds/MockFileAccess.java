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
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang.mutable.MutableInt;

import com.datatorrent.common.util.Pair;
import com.datatorrent.common.util.Slice;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * File storage for testing.
 */
public class MockFileAccess extends HDSFileAccessFSImpl
{
  private final transient Kryo kryo = new Kryo();

  private final Set<String> deletedFiles = Sets.newHashSet();

  @Override
  public void delete(long bucketKey, String fileName) throws IOException
  {
    super.delete(bucketKey, fileName);
    deletedFiles.add(""+bucketKey+fileName);
  }

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
      public boolean seek(Slice key) throws IOException {
        Pair<byte[], Integer> v = data.get(key);
        if (v == null) {
          index.setValue(0);
          return false;
        }
        index.setValue(v.second);
        return true;
      }

      @Override
      public boolean next(Slice key, Slice value) throws IOException {

        if (deletedFiles.contains(""+bucketKey+fileName)) {
          throw new IOException("Simulated error for deleted file: " + fileName);
        }

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
          return true;
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

}
