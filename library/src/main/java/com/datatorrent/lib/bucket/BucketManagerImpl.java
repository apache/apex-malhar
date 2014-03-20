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
package com.datatorrent.lib.bucket;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;

import com.datatorrent.common.util.DTThrowable;

/**
 * A {@link BucketManager} implementation.
 * <p>
 * Configurable properties of the BucketManagerImpl.<br/>
 * <ol>
 * <li>
 * {@link #noOfBuckets}: total number of buckets.
 * </li>
 * <li>
 * {@link #noOfBucketsInMemory}: number of buckets that will be kept in memory. This limit is not strictly maintained.<br/>
 * Whenever a new bucket from disk is loaded, the manager checks whether this limit is reached. If it has then it finds the
 * least recently used (lru) bucket. If the lru bucket was accessed within last {@link #millisPreventingBucketEviction}
 * then it is NOT off-loaded otherwise it is removed from memory.
 * </li>
 * <li>
 * {@link #maxNoOfBucketsInMemory}: this is a hard limit that is enforced on number of buckets in the memory.<br/>
 * When this limit is reached, the lru bucket is off-loaded irrespective of its last accessed time.
 * </li>
 * <li>
 * {@link #millisPreventingBucketEviction}: duration in milliseconds which could prevent a bucket from being
 * offloaded.
 * </li>
 * <li>
 * {@link #writeEventKeysOnly}: when this is true, the manager would not cache the event. It will only
 * keep the event key. This reduces memory usage and is useful for operators like De-duplicator which are interested only
 * in the event key.
 * </li>
 * </ol>
 * </p>
 *
 * @param <T> event type
 */
public class BucketManagerImpl<T extends BucketEvent> implements BucketManager<T>, Runnable
{
  public static int DEF_NUM_BUCKETS_MEM = 100;
  public static long DEF_MILLIS_PREVENTING_EVICTION = 5 * 60000;
  private static long RESERVED_BUCKET_KEY = -2;
  //Check-pointed
  @Min(1)
  protected int noOfBuckets;
  @Min(1)
  protected int noOfBucketsInMemory;
  @Min(1)
  protected int maxNoOfBucketsInMemory;
  @Min(0)
  protected long millisPreventingBucketEviction;
  protected boolean writeEventKeysOnly;
  @Nonnull
  protected BucketStore<T> bucketStore;
  @Nonnull
  private final Map<Integer, Bucket<T>> dirtyBuckets;
  protected long committedWindow;

  //Not check-pointed
  //Indexed by bucketKey keys.
  protected transient Bucket<T>[] buckets;
  @Nonnull
  protected transient Set<Integer> evictionCandidates;
  protected transient Listener<T> listener;
  @Nonnull
  private transient final BlockingQueue<Long> eventQueue;

  private transient volatile boolean running;
  @Nonnull
  private transient final Lock lock;
  @Nonnull
  private transient final MinMaxPriorityQueue<Bucket<T>> bucketHeap;

  BucketManagerImpl()
  {
    eventQueue = new LinkedBlockingQueue<Long>();
    evictionCandidates = Sets.newHashSet();
    dirtyBuckets = Maps.newConcurrentMap();
    bucketHeap = MinMaxPriorityQueue.orderedBy(new Comparator<Bucket<T>>()
    {
      @Override
      public int compare(Bucket<T> bucket1, Bucket<T> bucket2)
      {
        if (bucket1.lastUpdateTime() < bucket2.lastUpdateTime()) {
          return -1;
        }
        if (bucket1.lastUpdateTime() > bucket2.lastUpdateTime()) {
          return 1;
        }
        return 0;
      }
    }).create();
    lock = new Lock();
    committedWindow = -1;
  }

  protected BucketManagerImpl(Builder<T> builder)
  {
    this();
    this.writeEventKeysOnly = builder.writeEventKeysOnly;
    this.noOfBuckets = builder.noOfBuckets;
    this.noOfBucketsInMemory = builder.noOfBucketsInMemory;
    this.millisPreventingBucketEviction = builder.millisPreventingBucketEviction;
    this.maxNoOfBucketsInMemory = builder.maxNoOfBucketsInMemory;
    this.bucketStore = builder.bucketStore;
  }

  @Override
  public void shutdownService()
  {
    running = false;
    bucketStore.teardown();
  }

  @Override
  public long getBucketKeyFor(T event)
  {
    return event.getEventKey().hashCode() / noOfBuckets;
  }

  @Override
  public void run()
  {
    running = true;
    try {
      while (running) {
        Long request = eventQueue.poll(1, TimeUnit.SECONDS);
        if (request != null) {
          long requestedKey = request;
          if (RESERVED_BUCKET_KEY == requestedKey) {
            synchronized (lock) {
              lock.notify();
            }
          }
          else {
            int bucketIdx = (int) requestedKey % noOfBuckets;
            Map<Object, T> bucketDataInStore = bucketStore.fetchBucket(bucketIdx);

            if (buckets[bucketIdx] != null && buckets[bucketIdx].bucketKey != requestedKey) {
              //Delete the old bucket in memory at that index.
              Bucket<T> oldBucket = buckets[bucketIdx];

              dirtyBuckets.remove(bucketIdx);
              evictionCandidates.remove(bucketIdx);
              buckets[bucketIdx] = null;

              listener.bucketOffLoaded(oldBucket.bucketKey);
              bucketStore.deleteBucket(bucketIdx);
              logger.debug("deleted bucket {} {}", oldBucket.bucketKey, bucketIdx);
            }

            //Delete the least recently used bucket in memory if the noOfBucketsInMemory threshold is reached.
            if (evictionCandidates.size() + 1 > noOfBucketsInMemory) {
              for (int anIndex : evictionCandidates) {
                Preconditions.checkNotNull(buckets[anIndex], anIndex);
                bucketHeap.add(buckets[anIndex]);
              }
              int overFlow = evictionCandidates.size() + 1 - noOfBucketsInMemory;
              while (overFlow-- >= 0) {
                Bucket<T> lruBucket = bucketHeap.poll();
                if (lruBucket == null) {
                  break;
                }
                int lruIdx = (int) lruBucket.bucketKey % noOfBuckets;

                if (dirtyBuckets.containsKey(lruIdx)) {
                  break;
                }
                if (((System.currentTimeMillis() - lruBucket.lastUpdateTime()) < millisPreventingBucketEviction) &&
                  ((evictionCandidates.size() + 1) <= maxNoOfBucketsInMemory)) {
                  break;
                }
                evictionCandidates.remove(lruIdx);
                buckets[lruIdx] = null;
                listener.bucketOffLoaded(lruBucket.bucketKey);
//                logger.debug("evicted bucket {} {}", lruBucket.bucketKey, lruIdx);
              }
            }

            Bucket<T> bucket = buckets[bucketIdx];
            if (bucket == null || bucket.bucketKey != requestedKey) {
//              logger.debug("create bucket main {}", requestedKey);
              bucket = new Bucket<T>(requestedKey);
              buckets[bucketIdx] = bucket;
            }
            bucket.setWrittenEvents(bucketDataInStore);
            evictionCandidates.add(bucketIdx);
            listener.bucketLoaded(bucket);
//            logger.debug("bucket resp {} {}", requestedKey, bucketIdx);

            bucketHeap.clear();
          }
        }
      }
    }
    catch (Throwable cause) {
      running = false;
      DTThrowable.rethrow(cause);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void startService(Context context, Listener<T> listener)
  {
    buckets = (Bucket<T>[]) Array.newInstance(Bucket.class, noOfBuckets);
    this.listener = Preconditions.checkNotNull(listener, "storageHandler");
    this.bucketStore.setup(context);

    //Create buckets for unwritten events which were check-pointed
    for (Map.Entry<Integer, Bucket<T>> bucketEntry : dirtyBuckets.entrySet()) {
      buckets[bucketEntry.getKey()] = bucketEntry.getValue();
    }
    Thread eventServiceThread = new Thread(this, "BucketLoaderService");
    eventServiceThread.start();
  }

  @Override
  public Bucket<T> getBucket(long bucketKey)
  {
    int bucketIdx = (int) bucketKey % noOfBuckets;
    Bucket<T> bucket = buckets[bucketIdx];
    if (bucket == null) {
      return null;
    }
    if (bucket.bucketKey == bucketKey) {
      bucket.updateAccessTime();
      return bucket;
    }
    return null;
  }

  @Override
  public void newEvent(long bucketKey, T event)
  {
    int bucketIdx = (int) bucketKey % noOfBuckets;

    Bucket<T> bucket = buckets[bucketIdx];

    if (bucket == null || bucket.bucketKey != bucketKey) {
//      logger.debug("creating bucket {}", bucketKey);
      bucket = new Bucket<T>(bucketKey);
      buckets[bucketIdx] = bucket;
      dirtyBuckets.put(bucketIdx, bucket);
    }
    if (dirtyBuckets.get(bucketIdx) == null) {
      dirtyBuckets.put(bucketIdx, bucket);
    }
    if (writeEventKeysOnly) {
      bucket.addNewEvent(event.getEventKey(), null);
    }
    else {
      bucket.addNewEvent(event.getEventKey(), event);
    }
  }

  @Override
  public void endWindow(long window)
  {
    Map<Integer, Map<Object, T>> dataToStore = Maps.newHashMap();

    for (Map.Entry<Integer, Bucket<T>> entry : dirtyBuckets.entrySet()) {
      Bucket<T> bucket = entry.getValue();
      dataToStore.put(entry.getKey(), bucket.getUnwrittenEvents());
      bucket.transferDataFromMemoryToStore();
      evictionCandidates.add(entry.getKey());
    }

    long start = System.currentTimeMillis();
    try {
      bucketStore.storeBucketData(window, dataToStore);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    logger.debug("store time {}", System.currentTimeMillis() - start);

    dirtyBuckets.clear();
    committedWindow = window;
  }

  @Override
  public void blockUntilAllRequestsServiced() throws InterruptedException
  {
    synchronized (lock) {
      loadBucketData(RESERVED_BUCKET_KEY);
      lock.wait();
    }
    Preconditions.checkArgument(eventQueue.size() == 0, eventQueue);
  }

  @Override
  public void loadBucketData(long bucketKey)
  {
//    logger.debug("bucket request {}", command);
    eventQueue.offer(bucketKey);
  }

  @Override
  public void definePartitions(List<BucketManager<T>> oldManagers, Map<Integer, BucketManager<T>> partitionKeysToManagers, int partitionMask)
  {
    for (BucketManager<T> manager : oldManagers) {
      BucketManagerImpl<T> managerImpl = (BucketManagerImpl<T>) manager;

      for (Map.Entry<Integer, Bucket<T>> bucketEntry : managerImpl.dirtyBuckets.entrySet()) {
        Bucket<T> sourceBucket = bucketEntry.getValue();
        int sourceBucketIdx = bucketEntry.getKey();

        for (Map.Entry<Object, T> eventEntry : sourceBucket.getUnwrittenEvents().entrySet()) {
          int partition = eventEntry.getKey().hashCode() & partitionMask;
          BucketManagerImpl<T> newManagerImpl = (BucketManagerImpl<T>) partitionKeysToManagers.get(partition);

          Bucket<T> destBucket = newManagerImpl.dirtyBuckets.get(sourceBucketIdx);
          if (destBucket == null) {
            destBucket = new Bucket<T>(sourceBucket.bucketKey);
            newManagerImpl.dirtyBuckets.put(sourceBucketIdx, destBucket);
          }
          destBucket.addNewEvent(eventEntry.getKey(), eventEntry.getValue());
        }
      }
    }
  }

  @Override
  public BucketManagerImpl<T> cloneWithProperties()
  {
    BucketManagerImpl<T> clone = this.newBuilder().build();
    clone.committedWindow = committedWindow;
    return clone;
  }

  @SuppressWarnings("ClassMayBeInterface")
  private static class Lock
  {
  }

  public static class Builder<T extends BucketEvent>
  {
    protected int noOfBuckets;
    protected int noOfBucketsInMemory;
    protected int maxNoOfBucketsInMemory;
    protected long millisPreventingBucketEviction;
    protected boolean writeEventKeysOnly;
    protected BucketStore<T> bucketStore;

    protected Builder(boolean writeEventKeysOnly)
    {
      this.writeEventKeysOnly = writeEventKeysOnly;
    }

    /**
     * Creates a BucketManagerImpl builder with following default values:<br/>
     * <ul>
     * <li>
     * {@link #noOfBucketsInMemory} =  if {@link #noOfBuckets} > {@value #DEF_NUM_BUCKETS_MEM} then {@value #DEF_NUM_BUCKETS_MEM};
     * otherwise {@link #noOfBuckets}
     * </li>
     *
     * <li>
     * {@link #maxNoOfBucketsInMemory} = {@link #noOfBucketsInMemory} * 2
     * </li>
     *
     * <li>
     * {@link #millisPreventingBucketEviction} = {@value #DEF_MILLIS_PREVENTING_EVICTION}
     * </li>
     *
     * <li>
     * {@link #bucketStore} = {@link HdfsBucketStore}
     * </li>
     * </ul>
     *
     * @param writeEventKeysOnly true for keeping only event keys in memory and store; false otherwise.
     * @param noOfBuckets        total no. of buckets.
     */
    public Builder(boolean writeEventKeysOnly, int noOfBuckets)
    {
      this.writeEventKeysOnly = writeEventKeysOnly;
      this.bucketStore = new HdfsBucketStore<T>(noOfBuckets, writeEventKeysOnly);
      this.noOfBucketsInMemory = noOfBuckets > DEF_NUM_BUCKETS_MEM ? DEF_NUM_BUCKETS_MEM : noOfBuckets;
      this.maxNoOfBucketsInMemory = noOfBucketsInMemory * 2;
      this.millisPreventingBucketEviction = DEF_MILLIS_PREVENTING_EVICTION;
      this.noOfBuckets = noOfBuckets;
    }

    public Builder<T> bucketStore(@Nonnull BucketStore<T> store)
    {
      this.bucketStore = store;
      return this;
    }

    /**
     * Sets the number of buckets which will be kept in memory.
     */
    public Builder<T> noOfBucketsInMemory(int noOfBucketsInMemory)
    {
      this.noOfBucketsInMemory = noOfBucketsInMemory;
      return this;
    }

    /**
     * Sets the hard limit on no. of buckets in memory.
     */
    public Builder<T> maxNoOfBucketsInMemory(int maxNoOfBucketsInMemory)
    {
      this.maxNoOfBucketsInMemory = maxNoOfBucketsInMemory;
      return this;
    }

    /**
     * Sets the duration in millis that prevent a bucket from being off-loaded.
     */
    public Builder<T> millisPreventingBucketEviction(long millisPreventingBucketEviction)
    {
      this.millisPreventingBucketEviction = millisPreventingBucketEviction;
      return this;
    }

    public BucketManagerImpl<T> build()
    {
      return new BucketManagerImpl<T>(this);
    }

  }

  protected Builder<T> newBuilder()
  {
    return new Builder<T>(writeEventKeysOnly, noOfBuckets)
      .noOfBucketsInMemory(noOfBucketsInMemory)
      .maxNoOfBucketsInMemory(maxNoOfBucketsInMemory)
      .millisPreventingBucketEviction(millisPreventingBucketEviction)
      .bucketStore(bucketStore);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BucketManagerImpl)) {
      return false;
    }

    BucketManagerImpl that = (BucketManagerImpl) o;

    if (committedWindow != that.committedWindow) {
      return false;
    }
    if (maxNoOfBucketsInMemory != that.maxNoOfBucketsInMemory) {
      return false;
    }
    if (millisPreventingBucketEviction != that.millisPreventingBucketEviction) {
      return false;
    }
    if (noOfBuckets != that.noOfBuckets) {
      return false;
    }
    if (noOfBucketsInMemory != that.noOfBucketsInMemory) {
      return false;
    }
    if (writeEventKeysOnly != that.writeEventKeysOnly) {
      return false;
    }
    if (!bucketStore.equals(that.bucketStore)) {
      return false;
    }
    return dirtyBuckets.equals(that.dirtyBuckets);

  }

  @Override
  public int hashCode()
  {
    int result = noOfBuckets;
    result = 31 * result + noOfBucketsInMemory;
    result = 31 * result + maxNoOfBucketsInMemory;
    result = 31 * result + (int) (millisPreventingBucketEviction ^ (millisPreventingBucketEviction >>> 32));
    result = 31 * result + (writeEventKeysOnly ? 1 : 0);
    result = 31 * result + (bucketStore.hashCode());
    result = 31 * result + (dirtyBuckets.hashCode());
    result = 31 * result + (int) (committedWindow ^ (committedWindow >>> 32));
    return result;
  }

  private static transient final Logger logger = LoggerFactory.getLogger(BucketManagerImpl.class);
}
