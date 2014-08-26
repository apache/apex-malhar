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
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.*;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

/**
 * Implements HFile backed Historic Data Store (HDS) key/value storage.
 *
 */
public class HFileImpl extends HDSFileAccessFSImpl {

  private transient CacheConfig cacheConfig = null;
  private Comparator<Slice> comparator = null;
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

  private KeyValue.KVComparator getKVComparator() {
    if (comparator == null) {
      return new KeyValue.RawBytesComparator();
    }
    return new ComparatorAdaptor(comparator);
  }

  public void setComparator(Comparator<Slice> comparator) {
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
    final KeyValue.KVComparator comparator = getKVComparator();
    final HFileContext context = getContext();
    final Configuration conf = getConfiguration();
    final HFile.Writer writer = HFile.getWriterFactory(conf, cacheConf)
            .withOutputStream(fsdos)
            .withComparator(comparator)
            .withFileContext(context)
            .create();
    ComparatorAdaptor.COMPARATOR.set(this.comparator);

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
        ComparatorAdaptor.COMPARATOR.remove();
      }
    };

  }

  @Override
  public HDSFileReader getReader(long bucketKey, String fileName) throws IOException
  {
    ComparatorAdaptor.COMPARATOR.set(this.comparator);
    //FSDataInputStream fsdis =  getInputStream(bucketKey, fileName);
    final Configuration conf = getConfiguration();
    final CacheConfig cacheConfig = getCacheConfig();
    final Path filePath = getFilePath(bucketKey, fileName);
    final HFile.Reader reader = HFile.createReader(fs, filePath, cacheConfig, conf);
    final HFileScanner scanner = reader.getScanner(true, true, false);

    {
      scanner.seekTo();
    }

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
      public boolean seek(byte[] key) throws IOException
      {
        if (scanner.seekTo(key) == 0){
          return true;
        } else {
          scanner.next();
          return false;
        }
      }

      @Override
      public boolean next(Slice key, Slice value) throws IOException {

        if (reader.getEntries() <= 0) return false;

        KeyValue kv = scanner.getKeyValue();
        if (kv == null) {
          // cursor is already at the end
          return false;
        }
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
        ComparatorAdaptor.COMPARATOR.remove();
      }

    };
  }

  public static class ComparatorAdaptor extends KVComparator
  {
    final Slice s1 = new Slice(null, 0, 0);
    final Slice s2 = new Slice(null, 0, 0);
    final Comparator<Slice> cmp;

    static final ThreadLocal<Comparator<Slice>> COMPARATOR = new ThreadLocal<Comparator<Slice>>();

    public ComparatorAdaptor()
    {
      cmp = COMPARATOR.get();
      Preconditions.checkNotNull(cmp, "Key comparator must be set as ThreadLocal.");
    }

    public ComparatorAdaptor(Comparator<Slice> cmp)
    {
      this.cmp = cmp;
    }

    @Override
    public int compare(byte[] l, int loff, int llen, byte[] r, int roff, int rlen)
    {
      s1.buffer = l;
      s1.offset = loff;
      s1.length = llen;
      s2.buffer = r;
      s2.offset = roff;
      s2.length = rlen;
      return cmp.compare(s1,  s2);
    }

    @Override
    public int compareFlatKey(byte[] left, int loffset, int llength, byte[] right, int roffset, int rlength)
    {
      s1.buffer = left;
      s1.offset = loffset;
      s1.length = llength;
      s2.buffer = right;
      s2.offset = roffset;
      s2.length = rlength;
      return cmp.compare(s1,  s2);
    }

  }

}
