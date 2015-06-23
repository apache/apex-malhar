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
package com.datatorrent.contrib.hdht.hfile;

import com.datatorrent.netlet.util.Slice;
import com.datatorrent.contrib.hdht.HDHTFileAccessFSImpl;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Implements HFile backed Historic Data Store (HDHT) key/value storage access.
 *
 * Key tuning parameters include data block size, which can affect caching and random access performance, and
 * use of compression, which can affect access speed and storage space used.
 *
 *   blockSize - Size of data blocks in bytes.  Default: 65536 ( 64 * 1024 bytes)
 *   compression - Compression algorithm to use.  Options: none, gz, lz4, lzo, snappy.  Default: none
 *
 * These parameters can be set with following property definitions.  (replace HDSOutputOperator with actual operator name)
 *
 *   <property>
 *     <name>dt.operator.HDSOutputOperator.prop.fileStore.blockSize</name>
 *     <value>1048576</value>
 *   </property>
 *   <property>
 *     <name>dt.operator.HDSOutputOperator.prop.fileStore.compression</name>
 *     <value>1048576</value>
 *   </property>
 *
 * A number of configuration properties can be specified to tune the performance of this implementation.
 * Here are some of the properties which can be configured:
 *
 *   hfile.block.cache.size - percentage of maximum java heap to allocate to block cache.  Default: 0.25 (25%)
 *   hbase.bucketcache.size - percent of maximum java heap to allocate to bucketcache, or value in megabytes.  Default:
 *   hbase.bucketcache.combinedcache.enabled - indices and blooms are kept in the LRU blockcache and the data blocks are kept in the bucketcache.  Default: true
 *   hbase.bucketcache.percentage.in.combinedcache - percent to dedicate to bucketcache vs blockcache.  Default: 0.9 ( 90% )
 *   hbase.rs.cacheblocksonwrite - cache data blocks on write.  Default: false
 *   hfile.block.index.cacheonwrite - cache index blocks on write.  Default: false
 *   hbase.rs.evictblocksonclose - evict all blocks from cache when file is closed.  Default: false
 *
 * These properties can be defined using global Hadoop configuration (available on all Hadoop nodes) or through
 * configProperties helper.  In following example configProperties is used to set the values.  (replace HDSOutputOperator with actual operator name)
 *
 *   <property>
 *     <name>dt.operator.HDSOutputOperator.prop.fileStore.configProperties(hfile.block.cache.size)</name>
 *     <value>0.25</value>
 *   </property>
 *
 * @since 2.0.0
 */
public class HFileImpl extends HDHTFileAccessFSImpl {

  private static final Logger LOG = LoggerFactory.getLogger(HFileImpl.class);
  private transient CacheConfig cacheConfig = null;
  private Comparator<Slice> comparator = null;
  private transient HFileContext context = null;
  private Properties configProperties = new Properties();
  private int blockSize = 65536;
  private String compression = "none";


  /**
   * Get HFile cache configuration based on combination of global settings and configProperties.
   * @return The cache config.
   */
  protected CacheConfig getCacheConfig() {
    if (cacheConfig == null ) {
      cacheConfig = new CacheConfig(getConfiguration());
      LOG.debug("Cache config: {}", cacheConfig);
      LOG.debug("Block cache capacity: {}m", (cacheConfig.getBlockCache().getFreeSize()-cacheConfig.getBlockCache().size())/(1024*1024));
    }
    return cacheConfig;
  }

  protected void setCacheConfig(CacheConfig conf) {
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


  public Properties getConfigProperties() {
    return configProperties;
  }

  public void setConfigProperties(Properties configProperties) {
    this.configProperties = configProperties;
  }

  public String getCompression() {
    return compression;
  }

  public void setCompression(String compression) {
    this.compression = compression;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public void setBlockSize(int blockSize) {
    this.blockSize = blockSize;
  }

  /**
   * Creates new context with override options for compression, and blockSize.  To configure other
   * settings supported by {@link org.apache.hadoop.hbase.io.hfile.HFileContext} use setContext method.
   * @return The HFileContext.
   */
  public HFileContext buildContext() {
    return new HFileContextBuilder()
            .withCompression(Compression.getCompressionAlgorithmByName(getCompression()))
            .withBlockSize(getBlockSize())
            .build();
  }

  public HFileContext getContext() {
    if (context == null ) {
      context = buildContext();
    }
    return context;
  }

  public void setContext(HFileContext context) {
    this.context = context;
  }

  /**
   * Returns global {@link org.apache.hadoop.conf.Configuration} object with additional properties set based on
   * contents of configProperties.
   * @return The configuration.
   */
  public Configuration getConfiguration() {
    Configuration conf = fs.getConf();
    for (String name: configProperties.stringPropertyNames()) {
      String value = configProperties.getProperty(name);
      conf.set(name, value);
    }
    return fs.getConf();
  }


  /**
   * Creates and returns a new HFile writer.
   * @param bucketKey
   * @param fileName
   * @return The file writer.
   * @throws IOException
   */
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

  /**
   * Creates and returns a new HFile reader.
   * @param bucketKey
   * @param fileName
   * @return The reader.
   * @throws IOException
   */
  @Override
  public HDSFileReader getReader(long bucketKey, String fileName) throws IOException
  {
    ComparatorAdaptor.COMPARATOR.set(this.comparator);
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
          data.put(key, Arrays.copyOfRange(kv.getRowArray(), kv.getValueOffset(), kv.getValueOffset() + kv.getValueLength()));
        } while (scanner.next());
      }

      @Override
      public void reset() throws IOException {
        scanner.seekTo();
      }

      @Override
      public boolean seek(Slice key) throws IOException
      {
        if (scanner.seekTo(key.buffer, key.offset, key.length) == 0){
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

  /**
   * Adapter to support comparisons between two {@link com.datatorrent.netlet.util.Slice} objects.
   */
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
