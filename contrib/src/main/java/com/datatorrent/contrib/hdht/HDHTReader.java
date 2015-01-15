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
package com.datatorrent.contrib.hdht;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.HDHTFileAccess.HDSFileReader;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

/**
 * Reader for historical data store.
 * Implements asynchronous read from backing files and query refresh.
 *
 * @displayName HDHT Reader
 * @category Input
 * @tags hds, input operator
 */

public class HDHTReader implements Operator, HDHT.Reader
{
  public static final String FNAME_WAL = "_WAL";
  public static final String FNAME_META = "_META";

  public static class HDSQuery
  {
    public long bucketKey;
    public Slice key;
    public int keepAliveCount;
    public volatile byte[] result;
    public volatile boolean processed;

    @Override public String toString()
    {
      return "HDSQuery{" +
          "bucketKey=" + bucketKey +
          ", key=" + key +
          ", keepAliveCount=" + keepAliveCount +
          ", result=" + Arrays.toString(result) +
          ", processed=" + processed +
          '}';
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(HDHTReader.class);

  protected final transient Kryo kryo = new Kryo();
  @NotNull
  protected Comparator<Slice> keyComparator = new DefaultKeyComparator();
  @Valid
  @NotNull
  protected HDHTFileAccess store;

  public BucketMeta loadBucketMeta(long bucketKey)
  {
    BucketMeta bucketMeta = null;
    try {
      InputStream is = store.getInputStream(bucketKey, FNAME_META);
      bucketMeta = (BucketMeta)kryo.readClassAndObject(new Input(is));
      is.close();
    } catch (IOException e) {
      bucketMeta = new BucketMeta(keyComparator);
    }
    return bucketMeta;
  }

  /**
   * Map containing all current queries. Accessed by operator and reader threads.
   */
  protected final ConcurrentMap<Slice, HDSQuery> queries = Maps.newConcurrentMap();
  private final transient Map<Long, BucketReader> buckets = Maps.newHashMap();

  @VisibleForTesting
  protected transient ExecutorService queryExecutor;
  private volatile transient Exception executorError;

  public HDHTReader()
  {
  }

  /**
   * Compare keys for sequencing as secondary level of organization within buckets.
   * In most cases it will be implemented using a time stamp as leading component.
   * @return The key comparator.
   */
  public Comparator<Slice> getKeyComparator()
  {
    return keyComparator;
  }

  public void setKeyComparator(Comparator<Slice> keyComparator)
  {
    this.keyComparator = keyComparator;
  }


  public HDHTFileAccess getFileStore()
  {
    return store;
  }

  public void setFileStore(HDHTFileAccess fileStore)
  {
    this.store = fileStore;
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.store.init();
    if (queryExecutor == null) {
      queryExecutor = Executors.newSingleThreadScheduledExecutor(new NameableThreadFactory(this.getClass().getSimpleName()+"-Reader"));
    }
  }

  @Override
  public void teardown()
  {
    for (BucketReader bucket : this.buckets.values()) {
      IOUtils.closeQuietly(bucket);
    }
    IOUtils.closeQuietly(store);
    queryExecutor.shutdown();
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    Iterator<Map.Entry<Slice, HDSQuery>> it = this.queries.entrySet().iterator();
    while (it.hasNext()) {
      HDSQuery query = it.next().getValue();
      if (!query.processed) {
        processQuery(query);
      }
      // could be processed directly
      if (query.processed) {
        emitQueryResult(query);
        if (--query.keepAliveCount < 0) {
          //LOG.debug("Removing expired query {}", query);
          it.remove(); // query expired
        }
      }
    }
    if (executorError != null) {
      throw new RuntimeException("Error processing queries.", this.executorError);
    }
  }

  /**
   * Fetch result for the given query from persistent storage
   * Subclass can override this to serve from write cache.
   */
  protected void processQuery(final HDSQuery query)
  {
    Runnable readerRunnable = new Runnable() {
      @Override
      public void run()
      {
        try {
          LOG.debug("Processing {}", query);
          query.result = get(query.bucketKey, query.key);
          query.processed = true;
        } catch (Exception e) {
          executorError = e;
        }
      }
    };
    this.queryExecutor.execute(readerRunnable);
  }

  protected BucketReader getReader(long bucketKey)
  {
    BucketReader br = this.buckets.get(bucketKey);
    if (br == null) {
      this.buckets.put(bucketKey, br = new BucketReader());
    }
    // meta data can be invalidated on write without removing unaffected readers
    if (br.bucketMeta == null) {
      LOG.debug("Reading {} {}", bucketKey, FNAME_META);
      br.bucketMeta = loadBucketMeta(bucketKey);
    }
    return br;
  }

  protected void invalidateReader(long bucketKey, Set<String> fileNames)
  {
    BucketReader bucket = this.buckets.get(bucketKey);
    if (bucket != null) {
      bucket.bucketMeta = null; // force index reload
      for (String name : fileNames) {
        LOG.debug("Closing reader {}", name);
        IOUtils.closeQuietly(bucket.readers.remove(name));
      }
    }
  }

  private static Slice GET_KEY = new Slice(null, 0, 0);

  @Override
  public byte[] get(long bucketKey, Slice key) throws IOException
  {
    for (int i=0; i<10; i++) {
      BucketReader bucket = getReader(bucketKey);
      BucketMeta bucketMeta = bucket.bucketMeta;
      if (bucketMeta == null) {
        // meta data invalidated
        continue;
      }

      Map.Entry<Slice, BucketFileMeta> floorEntry = bucket.bucketMeta.files.floorEntry(key);
      if (floorEntry == null) {
        // no file for this key
        return null;
      }

      try {
        HDSFileReader reader = bucket.readers.get(floorEntry.getValue().name);
        if (reader == null) {
          LOG.debug("Opening file {} {}", bucketKey, floorEntry.getValue().name);
          bucket.readers.put(floorEntry.getValue().name, reader = store.getReader(bucketKey, floorEntry.getValue().name));
        }
        Slice value = new Slice(null, 0,0);
        if (reader.seek(key)) {
          reader.next(GET_KEY, value);
        }
        if (value.offset == 0) {
          return value.buffer;
        } else {
          // this is inefficient, should return Slice
          return Arrays.copyOfRange(value.buffer, value.offset, value.offset + value.length);
        }
      } catch (IOException e) {
        // check for meta file update
        this.buckets.remove(bucketKey);
        bucket.close();
        bucket = getReader(bucketKey);
        Map.Entry<Slice, BucketFileMeta> newEntry = bucket.bucketMeta.files.floorEntry(key);
        if (newEntry != null && newEntry.getValue().name.compareTo(floorEntry.getValue().name) == 0) {
          // file still the same - error unrelated to rewrite
          throw e;
        }
        // retry
        LOG.debug("Retry after meta data change bucket {} from {} to {}", bucketKey, floorEntry, newEntry);
        continue;
      }
    }
    return null;
  }

  protected void addQuery(HDSQuery query)
  {
    HDSQuery existingQuery = this.queries.get(query.key);
    if (existingQuery != null) {
      query.keepAliveCount = Math.max(query.keepAliveCount, existingQuery.keepAliveCount);
    }
    this.queries.put(query.key, query);
  }

  protected void emitQueryResult(HDSQuery query)
  {
  }

  /**
   * Default key comparator that performs lexicographical comparison of the byte arrays.
   */
  public static class DefaultKeyComparator implements Comparator<Slice>
  {
    @Override
    public int compare(Slice o1, Slice o2)
    {
      return WritableComparator.compareBytes(o1.buffer, o1.offset, o1.length, o2.buffer, o2.offset, o2.length);
    }
  }

  public static class BucketFileMeta
  {
    /**
     * Name of file (relative to bucket)
     */
    public String name;
    /**
     * Lower bound sequence key
     */
    public Slice startKey;

    @Override
    public String toString()
    {
      return "BucketFileMeta [name=" + name + ", startKey=" + startKey + "]";
    }
  }

  /**
   * Meta data about bucket, persisted in store
   * Flushed on compaction
   */
  public static class BucketMeta
  {
    protected BucketMeta(Comparator<Slice> cmp)
    {
      files = new TreeMap<Slice, BucketFileMeta>(cmp);
    }

    @SuppressWarnings("unused")
    private BucketMeta()
    {
      // for serialization only
      files = null;
    }

    protected BucketFileMeta addFile(long bucketKey, Slice startKey)
    {
      BucketFileMeta bfm = new BucketFileMeta();
      bfm.name = Long.toString(bucketKey) + '-' + this.fileSeq++;
      if (startKey.length != startKey.buffer.length) {
        // normalize key for serialization
        startKey = new Slice(startKey.toByteArray());
      }
      bfm.startKey = startKey;
      files.put(startKey, bfm);
      return bfm;
    }

    int fileSeq;
    long committedWid;
    final TreeMap<Slice, BucketFileMeta> files;
    HDHTWalManager.WalPosition recoveryStartWalPosition;
  }

  private static class BucketReader implements Closeable
  {
    BucketMeta bucketMeta;
    final HashMap<String, HDSFileReader> readers = Maps.newHashMap();

    @Override
    public void close() throws IOException
    {
      for (HDSFileReader reader : readers.values()) {
        reader.close();
      }
    }
  }

}
