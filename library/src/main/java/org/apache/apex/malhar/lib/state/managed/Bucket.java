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
package org.apache.apex.malhar.lib.state.managed;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

import com.datatorrent.lib.fileaccess.FileAccess;
import com.datatorrent.netlet.util.Slice;

/**
 * A bucket that groups events.
 *
 * @since 3.4.0
 */
public interface Bucket extends ManagedStateComponent
{
  /**
   * @return bucket id
   */
  long getBucketId();

  /**
   *
   * @return size of bucket in memory.
   */
  long getSizeInBytes();

  /**
   * Get value of a key.
   *
   * @param key        key.
   * @param timeBucket time bucket of the key if known; -1 otherwise.
   * @param source     source to read from
   * @return bucketed value of the key.
   */
  BucketedValue get(Slice key, long timeBucket, ReadSource source);

  /**
   * Set value of a key.
   *
   * @param key        key.
   * @param timeBucket timeBucket of the key.
   * @param value      value of the key.
   * @param time       time associated with event.
   */
  void put(Slice key, long timeBucket, Slice value, long time);

  /**
   * Triggers the bucket to checkpoint. Returns the non checkpointed data so far.
   *
   * @return non checkpointed data.
   */
  Map<Slice, BucketedValue> checkpoint(long windowId);

  /**
   * Triggers the bucket to commit data till provided window id.
   *
   * @param windowId window id
   */
  void committed(long windowId);

  /**
   * Triggers bucket to free memory which is already persisted in bucket data files.
   *
   * @param windowId All the data corresponding to windowId's less than or equal to this winowId
   * will be freed.
   * @return amount of memory freed in bytes.
   * @throws IOException
   */
  long freeMemory(long windowId) throws IOException;

  /**
   * Allows the bucket to process/cache data which is recovered (from window files) after failure.
   *
   * @param windowId recovery window
   * @param recoveredData recovered data
   */
  void recoveredData(long windowId, Map<Slice, Bucket.BucketedValue> recoveredData);

  enum ReadSource
  {
    MEMORY,      //state in memory in key/value form
    READERS,     //these are streams in which the key will be searched and serialized.
    ALL          //both the above states.
  }

  class BucketedValue
  {
    private long timeBucket;
    private Slice value;
    private long latestTime;

    protected BucketedValue()
    {
    }

    protected BucketedValue(long timeBucket, Slice value, long latestTime)
    {
      this.timeBucket = timeBucket;
      this.value = value;
      this.latestTime = latestTime;
    }

    protected long getTimeBucket()
    {
      return timeBucket;
    }

    protected void setTimeBucket(long timeBucket)
    {
      this.timeBucket = timeBucket;
    }

    protected long getLatestTime()
    {
      return latestTime;
    }

    protected void setLatestTime(long time)
    {
      this.latestTime = time;
    }

    public Slice getValue()
    {
      return value;
    }

    public void setValue(Slice value)
    {
      this.value = value;
    }

    public long getSize()
    {
      long size = 0;
      if (value != null) {
        size += value.length;
      }
      size += Longs.BYTES * 2; //latest time & time-bucket
      return size;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BucketedValue)) {
        return false;
      }

      BucketedValue that = (BucketedValue)o;

      return timeBucket == that.timeBucket && value.equals(that.value);

    }

    @Override
    public int hashCode()
    {
      return Objects.hash(timeBucket, value);
    }

    public static Slice serialize(BucketedValue bucketedValue)
    {
      byte[] serializedVal = new byte[Longs.BYTES + bucketedValue.value.length];
      byte[] latestTimeBytes = Longs.toByteArray(bucketedValue.latestTime);

      System.arraycopy(latestTimeBytes, 0, serializedVal, 0, latestTimeBytes.length);
      System.arraycopy(bucketedValue.value.buffer, bucketedValue.value.offset, serializedVal, latestTimeBytes.length,
          bucketedValue.value.length);

      return new Slice(serializedVal);
    }

    public static BucketedValue deserialize(Slice serializedSlice, long timeBucket)
    {
      BucketedValue bucketedValue = new BucketedValue();

      byte[] latestTimeBytes = new byte[Longs.BYTES];
      System.arraycopy(serializedSlice.buffer, serializedSlice.offset, latestTimeBytes, 0, Longs.BYTES);
      bucketedValue.latestTime = Longs.fromByteArray(latestTimeBytes);

      //not creating a new slice instance but just modifying the offset and length
      serializedSlice.offset += Longs.BYTES;
      serializedSlice.length -= Longs.BYTES;
      bucketedValue.value = serializedSlice;

      bucketedValue.timeBucket = timeBucket;
      return bucketedValue;
    }

  }

  /**
   * Default bucket.<br/>
   * Not thread-safe.
   */
  class DefaultBucket implements Bucket
  {
    private final long bucketId;

    //Key -> Ordered values
    private transient Map<Slice, BucketedValue> flash = Maps.newConcurrentMap();

    //Data persisted in write ahead logs. window -> bucket
    private final transient ConcurrentSkipListMap<Long, Map<Slice, BucketedValue>> checkpointedData =
        new ConcurrentSkipListMap<>();

    //Data persisted in bucket data files
    private final transient ConcurrentSkipListMap<Long, Map<Slice, BucketedValue>> committedData =
        new ConcurrentSkipListMap<>();

    //Data serialized/deserialized from bucket data files: key -> value from latest time bucket on file
    private final transient Map<Slice, BucketedValue> fileCache = Maps.newConcurrentMap();

    //TimeBucket -> FileReaders
    private final transient Map<Long, FileAccess.FileReader> readers = Maps.newTreeMap();

    protected transient ManagedStateContext managedStateContext;

    private AtomicLong sizeInBytes = new AtomicLong(0);

    private final transient Slice dummyGetKey = new Slice(null, 0, 0);

    private transient TreeMap<Long, BucketsFileSystem.TimeBucketMeta> cachedBucketMetas;

    private DefaultBucket()
    {
      //for kryo
      bucketId = -1;
    }

    protected DefaultBucket(long bucketId)
    {
      this.bucketId = bucketId;
    }

    @Override
    public void setup(@NotNull ManagedStateContext managedStateContext)
    {
      this.managedStateContext = Preconditions.checkNotNull(managedStateContext, "managed state context");
    }

    @Override
    public long getBucketId()
    {
      return bucketId;
    }

    @Override
    public long getSizeInBytes()
    {
      return sizeInBytes.longValue();
    }

    private BucketedValue getFromMemory(Slice key)
    {
      //search the cache for key
      BucketedValue bucketedValue = flash.get(key);
      if (bucketedValue != null) {
        return bucketedValue;
      }

      for (Long window : checkpointedData.descendingKeySet()) {
        //traverse the checkpointed data in reverse order
        bucketedValue = checkpointedData.get(window).get(key);
        if (bucketedValue != null) {
          return bucketedValue;
        }
      }

      for (Long window : committedData.descendingKeySet()) {
        //traverse the committed data in reverse order
        bucketedValue = committedData.get(window).get(key);
        if (bucketedValue != null) {
          return bucketedValue;
        }
      }

      bucketedValue = fileCache.get(key);
      if (bucketedValue != null) {
        return bucketedValue;
      }

      return null;
    }

    private BucketedValue getFromReaders(Slice key, long timeBucket)
    {
      try {
        if (cachedBucketMetas == null) {
          cachedBucketMetas = managedStateContext.getBucketsFileSystem().getAllTimeBuckets(bucketId);
        }
        if (timeBucket != -1) {
          BucketedValue bucketedValue = getValueFromTimeBucketReader(key, timeBucket);
          if (bucketedValue != null) {
            if (timeBucket == cachedBucketMetas.firstKey()) {
              //if the requested time bucket is the latest time bucket on file, the key/value is put in the file cache.
              sizeInBytes.getAndAdd(key.length + bucketedValue.getSize());
              fileCache.put(key, bucketedValue);
            }
          }
          return bucketedValue;
        } else {
          //search all the time buckets
          for (BucketsFileSystem.TimeBucketMeta immutableTimeBucketMeta : cachedBucketMetas.values()) {
            if (managedStateContext.getKeyComparator().compare(key, immutableTimeBucketMeta.getFirstKey()) >= 0) {
              //keys in the time bucket files are sorted so if the first key in the file is greater than the key being
              //searched, the key will not be present in that file.
              BucketedValue bucketedValue = getValueFromTimeBucketReader(key, immutableTimeBucketMeta.getTimeBucketId());
              if (bucketedValue != null) {
                //Only when the key is read from the latest time bucket on the file, the key/value is put in the file
                // cache.
                sizeInBytes.getAndAdd(key.length + bucketedValue.getSize());
                fileCache.put(key, bucketedValue);
                return bucketedValue;
              }
            }
          }
          return null;
        }

      } catch (IOException e) {
        throw new RuntimeException("get time-buckets " + bucketId, e);
      }
    }

    @Override
    public BucketedValue get(Slice key, long timeBucket, ReadSource readSource)
    {
      switch (readSource) {
        case MEMORY:
          return getFromMemory(key);
        case READERS:
          return getFromReaders(key, timeBucket);
        case ALL:
        default:
          BucketedValue bucketedValue = getFromMemory(key);
          if (bucketedValue != null) {
            return bucketedValue;
          }
          return getFromReaders(key, timeBucket);
      }
    }

    /**
     * Returns the value for the key from a time-bucket reader
     * @param key        key
     * @param timeBucket time bucket
     * @return value if key is found in the time bucket; false otherwise
     */
    private BucketedValue getValueFromTimeBucketReader(Slice key, long timeBucket)
    {
      FileAccess.FileReader fileReader = readers.get(timeBucket);
      if (fileReader != null) {
        return readValue(fileReader, key, timeBucket);
      }
      //file reader is not loaded and is null
      try {
        if (loadFileReader(timeBucket)) {
          return readValue(readers.get(timeBucket), key, timeBucket);
        }
        return null;
      } catch (IOException e) {
        throw new RuntimeException("while loading " + bucketId + ", " + timeBucket, e);
      }
    }

    private BucketedValue readValue(FileAccess.FileReader fileReader, Slice key, long timeBucket)
    {
      Slice valSlice = new Slice(null, 0, 0);
      try {
        if (fileReader.seek(key)) {
          fileReader.next(dummyGetKey, valSlice);
          return BucketedValue.deserialize(valSlice, timeBucket);
        } else {
          return null;
        }
      } catch (IOException e) {
        throw new RuntimeException("reading " + bucketId + ", " + timeBucket, e);
      }
    }

    private boolean loadFileReader(long timeBucketId) throws IOException
    {
      BucketsFileSystem.TimeBucketMeta tbm = managedStateContext.getBucketsFileSystem()
          .getTimeBucketMeta(bucketId, timeBucketId);

      if (tbm != null) {
        FileAccess.FileReader reader = managedStateContext.getBucketsFileSystem().getReader(bucketId,
            BucketsFileSystem.getFileName(timeBucketId));
        readers.put(timeBucketId, reader);
        sizeInBytes.getAndAdd(tbm.getSizeInBytes());
        return true;
      }
      return false;
    }

    @Override
    public void put(Slice key, long timeBucket, Slice value, long time)
    {
      BucketedValue bucketedValue = flash.get(key);
      if (bucketedValue == null) {
        bucketedValue = new BucketedValue(timeBucket, value, time);
        flash.put(key, bucketedValue);
        sizeInBytes.getAndAdd(key.length + value.length + Longs.BYTES * 2);
      } else {
        if (timeBucket > bucketedValue.getTimeBucket() ||
            (timeBucket == bucketedValue.getTimeBucket() && time >= bucketedValue.getLatestTime())) {

          int inc = null == bucketedValue.getValue() ? value.length : value.length - bucketedValue.getValue().length;
          sizeInBytes.getAndAdd(inc);
          bucketedValue.setTimeBucket(timeBucket);
          bucketedValue.setValue(value);
          bucketedValue.setLatestTime(time);
        } else {
          LOG.warn("ignoring {} {} {} {}; existing {} {} {}", key, value, timeBucket, time,
              bucketedValue.getValue(), bucketedValue.getTimeBucket(), bucketedValue.getLatestTime());
        }
      }
    }

    @Override
    public long freeMemory(long windowId) throws IOException
    {
      long memoryFreed = 0;
      Long clearWindowId;

      while ((clearWindowId = committedData.floorKey(windowId)) != null) {
        Map<Slice, BucketedValue> windowData = committedData.remove(clearWindowId);

        for (Map.Entry<Slice, BucketedValue> entry: windowData.entrySet()) {
          memoryFreed += entry.getKey().length + entry.getValue().getSize();
        }
      }

      for (Map.Entry<Slice, BucketedValue> fileCacheEntry : fileCache.entrySet()) {
        memoryFreed += fileCacheEntry.getKey().length + fileCacheEntry.getValue().getSize();
      }
      fileCache.clear();
      if (cachedBucketMetas != null) {

        for (BucketsFileSystem.TimeBucketMeta tbm : cachedBucketMetas.values()) {
          FileAccess.FileReader reader = readers.remove(tbm.getTimeBucketId());
          if (reader != null) {
            memoryFreed += tbm.getSizeInBytes();
            reader.close();
          }
        }

      }
      sizeInBytes.getAndAdd(-memoryFreed);
      LOG.debug("space freed {} {}", bucketId, memoryFreed);
      return memoryFreed;
    }

    @Override
    public Map<Slice, BucketedValue> checkpoint(long windowId)
    {
      try {
        //transferring the data from flash to check-pointed state in finally block and re-initializing the flash.
        return flash;
      } finally {
        checkpointedData.put(windowId, flash);
        flash = Maps.newHashMap();
      }
    }

    @Override
    public void committed(long committedWindowId)
    {
      Iterator<Map.Entry<Long, Map<Slice, BucketedValue>>> stateIterator = checkpointedData.entrySet().iterator();

      while (stateIterator.hasNext()) {
        Map.Entry<Long, Map<Slice, BucketedValue>> entry = stateIterator.next();

        long savedWindow = entry.getKey();
        if (savedWindow <= committedWindowId) {
          Map<Slice, BucketedValue> bucketData = entry.getValue();

          long memoryFreed = 0;

          //removing any stale values from the file cache
          for (Slice key : bucketData.keySet()) {
            BucketedValue bucketedValueInFileCache = fileCache.remove(key);
            if (bucketedValueInFileCache != null) {
              memoryFreed += bucketedValueInFileCache.getSize() + key.length;
            }
          }

          for (BucketedValue bucketedValue : bucketData.values()) {
            FileAccess.FileReader reader = readers.get(bucketedValue.getTimeBucket());
            if (reader != null) {
              //closing the file reader for the time bucket if it is in memory because the time-bucket is modified
              //so it will be re-written by BucketsDataManager
              try {
                BucketsFileSystem.TimeBucketMeta tbm = cachedBucketMetas.get(bucketedValue.getTimeBucket());
                memoryFreed += tbm.getSizeInBytes();
                LOG.debug("closing reader {} {}", bucketId, bucketedValue.getTimeBucket());
                reader.close();
              } catch (IOException e) {
                throw new RuntimeException("closing reader " + bucketId + ", " + bucketedValue.getTimeBucket(), e);
              }
              readers.remove(bucketedValue.getTimeBucket());
            }
            if (readers.isEmpty()) {
              break;
            }
          }
          sizeInBytes.getAndAdd(-memoryFreed);
          committedData.put(savedWindow, bucketData);
          stateIterator.remove();
        } else {
          break;
        }
      }

      cachedBucketMetas = null;
    }

    @Override
    public void recoveredData(long recoveredWindow, Map<Slice, BucketedValue> data)
    {
      checkpointedData.put(recoveredWindow, data);
    }

    @Override
    public void teardown()
    {
      Set<Long> failureBuckets = Sets.newHashSet();
      for (Map.Entry<Long, FileAccess.FileReader> entry : readers.entrySet()) {
        try {
          LOG.debug("closing reader {} {}", bucketId, entry.getKey());
          entry.getValue().close();
        } catch (IOException e) {
          //will try to close all readers
          failureBuckets.add(entry.getKey());
        }
      }
      if (!failureBuckets.isEmpty()) {
        StringBuilder builder = new StringBuilder("teardown of ");
        builder.append(bucketId).append(" < ");
        for (Long timeBucket : failureBuckets) {
          builder.append(timeBucket);
        }
        builder.append(">");
        throw new RuntimeException(builder.toString());
      }
    }

    @VisibleForTesting
    Map<Long, FileAccess.FileReader> getReaders()
    {
      return readers;
    }

    @VisibleForTesting
    ConcurrentSkipListMap<Long, Map<Slice, BucketedValue>> getCommittedData()
    {
      return committedData;
    }

    @VisibleForTesting
    ConcurrentSkipListMap<Long, Map<Slice, BucketedValue>> getCheckpointedData()
    {
      return checkpointedData;
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBucket.class);
  }
}
