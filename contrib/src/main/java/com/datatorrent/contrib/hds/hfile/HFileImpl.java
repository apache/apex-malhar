/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.hds.hfile;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hds.HDSFileAccessFSImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.*;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Implements HFile backed Historic Data Store (HDS) key/value storage.
 *
 */
public class HFileImpl extends HDSFileAccessFSImpl {

  private transient CacheConfig cacheConfig = null;
  private transient KeyValue.KVComparator comparator = null;
  private transient HFileContext context = null;

  public CacheConfig getCacheConfig() {
    if (cacheConfig == null ) {
      cacheConfig = new CacheConfig(fs.getConf());
    }
    return cacheConfig;
  }

  public void setCacheConfig(CacheConfig conf) {
    this.cacheConfig = conf;
  }

  public KeyValue.KVComparator getComparator() {
    if (comparator == null) {
      comparator = new KeyValue.RawBytesComparator();
    }
    return comparator;
  }

  public void setComparator(KeyValue.KVComparator comparator) {
    this.comparator = comparator;
  }

  public HFileContext getContext() {
    if (context == null ) {
      context = new HFileContextBuilder().withCompression(Compression.Algorithm.NONE).build();
    }
    return context;
  }

  public void setContext(HFileContext context) {
    this.context = context;
  }

  public Configuration getConfiguration() {
    return fs.getConf();
  }


  @Override
  public HDSFileWriter getWriter(long bucketKey, String fileName) throws IOException
  {
    final FSDataOutputStream fsdos = getOutputStream(bucketKey, fileName);
    final CacheConfig cacheConf = getCacheConfig();
    final KeyValue.KVComparator comparator = getComparator();
    final HFileContext context = getContext();
    final Configuration conf = getConfiguration();
    final HFile.Writer writer = HFile.getWriterFactory(conf, cacheConf)
            .withOutputStream(fsdos)
            .withComparator(comparator)
            .withFileContext(context)
            .create();

    return new HDSFileWriter(){

      private long bytesAppendedCounter = 0;

      @Override
      public void append(byte[] key, byte[] value) throws IOException {
        bytesAppendedCounter += (key.length + value.length);
        writer.append(key, value);
      }

      @Override
      public long getBytesWritten() throws IOException {
        // Not accurate below HFile block size resolution due to flushing (and compression)
        // HFile block size is available via writer.getFileContext().getBlocksize()
        // bytesAppendedCounter is used to produce non-zero counts until first flush
        return (fsdos.getPos() <=0 ) ? bytesAppendedCounter : fsdos.getPos();
      }

      @Override
      public void close() throws IOException {
        writer.close();
        fsdos.close();
      }
    };

  }

  @Override
  public HDSFileReader getReader(long bucketKey, String fileName) throws IOException
  {
    //FSDataInputStream fsdis =  getInputStream(bucketKey, fileName);
    final Configuration conf = getConfiguration();
    final CacheConfig cacheConfig = getCacheConfig();
    final Path filePath = getFilePath(bucketKey, fileName);
    final HFile.Reader reader = HFile.createReader(fs, filePath, cacheConfig, conf);
    final HFileScanner scanner = reader.getScanner(true, true, false);

    return new HDSFileReader(){

      @Override
      public void readFully(TreeMap<Slice, byte[]> data) throws IOException {
        if (reader.getEntries() <= 0) return;
        scanner.seekTo();
        KeyValue kv;
        do {
          kv = scanner.getKeyValue();
          Slice key = new Slice(kv.getRowArray(), kv.getKeyOffset(), kv.getKeyLength());
          Slice value = new Slice(kv.getRowArray(), kv.getValueOffset(), kv.getValueLength());
          //data.put(key, value);
          data.put(key, value.clone().buffer);
        } while (scanner.next());
      }

      @Override
      public void reset() throws IOException {
        scanner.seekTo();
      }

      @Override
      public boolean seek(byte[] key) throws IOException {
        return (scanner.seekTo(key) == 0);
      }

      @Override
      public boolean next(Slice key, Slice value) throws IOException {

        if (reader.getEntries() <= 0) return false;

        KeyValue kv = scanner.getKeyValue();
        key.buffer = kv.getRowArray();
        key.offset = kv.getKeyOffset();
        key.length = kv.getKeyLength();
        value.buffer = kv.getRowArray();
        value.offset = kv.getValueOffset();
        value.length = kv.getValueLength();
        scanner.next();
        return true;
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

    };
  }


}
