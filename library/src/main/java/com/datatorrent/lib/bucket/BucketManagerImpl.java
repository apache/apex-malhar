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
 * @since 0.9.4
 */
public class BucketManagerImpl<T extends Bucketable> implements BucketManager<T>, Runnable
{
  public static int DEF_NUM_BUCKETS = 1000;
  public static int DEF_NUM_BUCKETS_MEM = 120;
  public static long DEF_MILLIS_PREVENTING_EVICTION = 10 * 60000;
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
  protected BucketStore<T> store;
  @Nonnull
  protected final Map<Integer, Bucket<T>> dirtyBuckets;
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
  protected transient BucketCounters bucketCounters;

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

    noOfBuckets = DEF_NUM_BUCKETS;
    noOfBucketsInMemory = DEF_NUM_BUCKETS_MEM;
    maxNoOfBucketsInMemory = DEF_NUM_BUCKETS_MEM + 100;
    millisPreventingBucketEviction = DEF_MILLIS_PREVENTING_EVICTION;
    writeEventKeysOnly = true;
  }

  /**
   * Sets the total number of buckets.
   *
   * @param noOfBuckets
   */
  public void setNoOfBuckets(int noOfBuckets)
  {
    this.noOfBuckets = noOfBuckets;
  }

  /**
   * Sets the number of buckets which will be kept in memory.
   */
  public void setNoOfBucketsInMemory(int noOfBucketsInMemory)
  {
    this.noOfBucketsInMemory = noOfBucketsInMemory;
  }

  /**
   * Sets the hard limit on no. of buckets in memory.
   */
  public void setMaxNoOfBucketsInMemory(int maxNoOfBucketsInMemory)
  {
    this.maxNoOfBucketsInMemory = maxNoOfBucketsInMemory;
  }

  /**
   * Sets the duration in millis that prevent a bucket from being off-loaded.
   */
  public void setMillisPreventingBucketEviction(long millisPreventingBucketEviction)
  {
    this.millisPreventingBucketEviction = millisPreventingBucketEviction;
  }

  /**
   * Set true for keeping only event keys in memory and store; false otherwise.
   *
   * @param writeEventKeysOnly
   */
  public void setWriteEventKeysOnly(boolean writeEventKeysOnly)
  {
    this.writeEventKeysOnly = writeEventKeysOnly;
  }

  @Nonnull
  public void setBucketCounters(@Nonnull BucketCounters bucketCounters)
  {
    this.bucketCounters = bucketCounters;
  }

  @Override
  public void shutdownService()
  {
    running = false;
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
            int bucketIdx = (int) (requestedKey % noOfBuckets);
            long numEventsRemoved = 0;
            if (buckets[bucketIdx] != null && buckets[bucketIdx].bucketKey != requestedKey) {
              //Delete the old bucket in memory at that index.
              Bucket<T> oldBucket = buckets[bucketIdx];

              dirtyBuckets.remove(bucketIdx);
              evictionCandidates.remove(bucketIdx);
              buckets[bucketIdx] = null;

              listener.bucketOffLoaded(oldBucket.bucketKey);
              store.deleteBucket(bucketIdx);
              if (bucketCounters != null) {
                bucketCounters.numDeletedBuckets++;
                bucketCounters.numBucketsInMemory--;
                numEventsRemoved += oldBucket.countOfUnwrittenEvents() + oldBucket.countOfWrittenEvents();
              }
              logger.debug("deleted bucket {} {}", oldBucket.bucketKey, bucketIdx);
            }

            Map<Object, T> bucketDataInStore = store.fetchBucket(bucketIdx);

            //Delete the least recently used bucket in memory if the noOfBucketsInMemory threshold is reached.
            if (evictionCandidates.size() + 1 > noOfBucketsInMemory) {

              for (int anIndex : evictionCandidates) {
                bucketHeap.add(buckets[anIndex]);
              }
              int overFlow = evictionCandidates.size() + 1 - noOfBucketsInMemory;
              while (overFlow-- >= 0) {
                Bucket<T> lruBucket = bucketHeap.poll();
                if (lruBucket == null) {
                  break;
                }
                int lruIdx = (int) (lruBucket.bucketKey % noOfBuckets);

                if (dirtyBuckets.containsKey(lruIdx)) {
                  break;
                }
                if (((System.currentTimeMillis() - lruBucket.lastUpdateTime()) < millisPreventingBucketEviction)
                  && ((evictionCandidates.size() + 1) <= maxNoOfBucketsInMemory)) {
                  break;
                }
                evictionCandidates.remove(lruIdx);
                buckets[lruIdx] = null;
                listener.bucketOffLoaded(lruBucket.bucketKey);
                if (bucketCounters != null) {
                  bucketCounters.numEvictedBuckets++;
                  bucketCounters.numBucketsInMemory--;
                  numEventsRemoved += lruBucket.countOfUnwrittenEvents() + lruBucket.countOfWrittenEvents();
                }
                logger.debug("evicted bucket {} {}", lruBucket.bucketKey, lruIdx);
              }
            }

            Bucket<T> bucket = buckets[bucketIdx];
            if (bucket == null || bucket.bucketKey != requestedKey) {
              bucket = new Bucket<T>(requestedKey);
              buckets[bucketIdx] = bucket;
            }
            bucket.setWrittenEvents(bucketDataInStore);
            evictionCandidates.add(bucketIdx);
            listener.bucketLoaded(bucket);
            if (bucketCounters != null) {
              synchronized (bucketCounters) {
                bucketCounters.numBucketsInMemory++;
                bucketCounters.numEventsInMemory -= numEventsRemoved;
                bucketCounters.numEventsInMemory += bucketDataInStore.size();
              }
            }
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
  public void initialize(@Nonnull BucketStore<T> store)
  {
    this.store = store;
    store.setNoOfBuckets(noOfBuckets);
    store.setWriteEventKeysOnly(writeEventKeysOnly);
  }

  @Override
  public void startService(Listener<T> listener)
  {
    Preconditions.checkArgument(store.isReady(), "store is not ready");
    logger.debug("bucket properties {}, {}, {}, {}", noOfBuckets, noOfBucketsInMemory, maxNoOfBucketsInMemory, millisPreventingBucketEviction);
    this.listener = Preconditions.checkNotNull(listener, "storageHandler");
    @SuppressWarnings("unchecked")
    Bucket<T>[] freshBuckets = (Bucket<T>[]) Array.newInstance(Bucket.class, noOfBuckets);
    buckets = freshBuckets;
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
    int bucketIdx = (int) (bucketKey % noOfBuckets);
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
    int bucketIdx = (int) (bucketKey % noOfBuckets);

    Bucket<T> bucket = buckets[bucketIdx];

    if (bucket == null || bucket.bucketKey != bucketKey) {
      bucket = new Bucket<T>(bucketKey);
      buckets[bucketIdx] = bucket;
      dirtyBuckets.put(bucketIdx, bucket);
    }
    else if (dirtyBuckets.get(bucketIdx) == null) {
      dirtyBuckets.put(bucketIdx, bucket);
    }

    bucket.addNewEvent(event.getEventKey(), writeEventKeysOnly ? null : event);
    if (bucketCounters != null) {
      synchronized (bucketCounters) {
        bucketCounters.numEventsInMemory++;
      }
    }
  }

  @Override
  public void endWindow(long window)
  {
    saveData(window, window);
  }

  protected void saveData(long window, long id)
  {
    Map<Integer, Map<Object, T>> dataToStore = Maps.newHashMap();
    long eventsCount = 0;
    for (Map.Entry<Integer, Bucket<T>> entry : dirtyBuckets.entrySet()) {
      Bucket<T> bucket = entry.getValue();
      dataToStore.put(entry.getKey(), bucket.getUnwrittenEvents());
      eventsCount += bucket.countOfUnwrittenEvents();
      bucket.transferDataFromMemoryToStore();
      evictionCandidates.add(entry.getKey());
    }
    if (bucketCounters != null) {
      bucketCounters.numEventsCommittedPerWindow = eventsCount;
    }
    try {
      if (!dataToStore.isEmpty()) {
        long start = System.currentTimeMillis();
        logger.debug("start store {}", window);
        store.storeBucketData(window, id, dataToStore);
        logger.debug("end store {} took {}", window, System.currentTimeMillis() - start);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
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
    BucketManagerImpl<T> clone = new BucketManagerImpl<T>();
    copyPropertiesTo(clone);
    return clone;
  }

  protected void copyPropertiesTo(BucketManagerImpl<T> other)
  {
    other.writeEventKeysOnly = writeEventKeysOnly;
    other.noOfBuckets = noOfBuckets;
    other.noOfBucketsInMemory = noOfBucketsInMemory;
    other.maxNoOfBucketsInMemory = maxNoOfBucketsInMemory;
    other.millisPreventingBucketEviction = millisPreventingBucketEviction;
    other.store = store;
    other.committedWindow = committedWindow;
  }

  @SuppressWarnings("ClassMayBeInterface")
  private static class Lock
  {
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
    if (!store.equals(that.store)) {
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
    result = 31 * result + (store.hashCode());
    result = 31 * result + (dirtyBuckets.hashCode());
    result = 31 * result + (int) (committedWindow ^ (committedWindow >>> 32));
    return result;
  }

  private static transient final Logger logger = LoggerFactory.getLogger(BucketManagerImpl.class);
}
