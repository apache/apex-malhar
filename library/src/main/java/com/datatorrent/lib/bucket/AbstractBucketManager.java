/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

import com.datatorrent.api.AutoMetric;
import com.datatorrent.lib.counters.BasicCounters;

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
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * This is the base implementation of BucketManager.
 * Subclasses must implement the createBucket method which creates the specific bucket based on bucketKey provided
 * and getBucketKeyFor method which calculates the bucket key of an event.
 * <p>
 * Configurable properties of the AbstractBucketManager.<br/>
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
public abstract class AbstractBucketManager<T> implements BucketManager<T>, Runnable
{
  public static int DEF_NUM_BUCKETS = 1000;
  public static int DEF_NUM_BUCKETS_MEM = 120;
  public static long DEF_MILLIS_PREVENTING_EVICTION = 10 * 60000;
  private static final long RESERVED_BUCKET_KEY = -2;
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
  @NotNull
  protected BucketStore<T> bucketStore;
  @NotNull
  protected final Map<Integer, AbstractBucket<T>> dirtyBuckets;
  protected long committedWindow;
  private boolean collateFilesForBucket = false;
  protected Set<Integer> bucketsToDelete;
  //Not check-pointed
  //Indexed by bucketKey keys.
  protected transient AbstractBucket<T>[] buckets;
  @NotNull
  protected transient Set<Integer> evictionCandidates;
  protected transient Listener<T> listener;
  @NotNull
  private transient final BlockingQueue<Long> eventQueue;
  private transient volatile boolean running;
  @NotNull
  private transient final Lock lock;
  @NotNull
  private transient final MinMaxPriorityQueue<AbstractBucket<T>> bucketHeap;

  protected transient boolean recordStats;
  protected transient BasicCounters<MutableLong> bucketCounters;

  // Auto Metrics
  @AutoMetric
  protected long deletedBuckets;
  @AutoMetric
  protected long bucketsInMemory;
  @AutoMetric
  protected long evictedBuckets;
  @AutoMetric
  protected long eventsInMemory;
  @AutoMetric
  protected long eventsCommittedLastWindow;
  @AutoMetric
  protected long endOfBuckets;
  @AutoMetric
  protected long startOfBuckets;

  public AbstractBucketManager()
  {
    eventQueue = new LinkedBlockingQueue<Long>();
    evictionCandidates = Sets.newHashSet();
    dirtyBuckets = Maps.newConcurrentMap();
    bucketHeap = MinMaxPriorityQueue.orderedBy(new Comparator<AbstractBucket<T>>()
    {
      @Override
      public int compare(AbstractBucket<T> bucket1, AbstractBucket<T> bucket2)
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
    bucketsToDelete = Sets.newHashSet();
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
   * @param noOfBucketsInMemory
   */
  public void setNoOfBucketsInMemory(int noOfBucketsInMemory)
  {
    this.noOfBucketsInMemory = noOfBucketsInMemory;
  }

  /**
   * Sets the hard limit on no. of buckets in memory.
   * @param maxNoOfBucketsInMemory
   */
  public void setMaxNoOfBucketsInMemory(int maxNoOfBucketsInMemory)
  {
    this.maxNoOfBucketsInMemory = maxNoOfBucketsInMemory;
  }

  /**
   * Sets the duration in millis that prevent a bucket from being off-loaded.
   * @param millisPreventingBucketEviction
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
    if (this.bucketStore != null) {
      this.bucketStore.setWriteEventKeysOnly(writeEventKeysOnly);
    }
  }

  public boolean isWriteEventKeysOnly()
  {
    return writeEventKeysOnly;
  }

  @Override
  public void setBucketCounters(@Nonnull BasicCounters<MutableLong> bucketCounters)
  {
    this.bucketCounters = bucketCounters;
    bucketCounters.setCounter(CounterKeys.BUCKETS_IN_MEMORY, new MutableLong());
    bucketCounters.setCounter(CounterKeys.EVICTED_BUCKETS, new MutableLong());
    bucketCounters.setCounter(CounterKeys.DELETED_BUCKETS, new MutableLong());
    bucketCounters.setCounter(CounterKeys.EVENTS_COMMITTED_LAST_WINDOW, new MutableLong());
    bucketCounters.setCounter(CounterKeys.EVENTS_IN_MEMORY, new MutableLong());
    recordStats = true;
  }

  @Override
  public void shutdownService()
  {
    running = false;
    bucketStore.teardown();
  }


  @Override
  public void run()
  {
    running = true;
    List<Long> requestedBuckets = Lists.newArrayList();
    // Evicted Buckets Map: Bucket Index -> Bucket Key.
    Map<Integer, Long> evictedBuckets = Maps.newHashMap();
    try {
      while (running) {
        Long request = eventQueue.poll(1, TimeUnit.SECONDS);
        if (request != null) {
          long requestedKey = request;
          if (RESERVED_BUCKET_KEY == requestedKey) {
            synchronized (lock) {
              lock.notify();
            }
            requestedBuckets.clear();
          }
          else {
            requestedBuckets.add(requestedKey);
            int bucketIdx = (int) (requestedKey % noOfBuckets);
            long numEventsRemoved = 0;
            if (buckets[bucketIdx] != null && buckets[bucketIdx].bucketKey != requestedKey) {
              //Delete the old bucket in memory at that index.
              AbstractBucket<T> oldBucket = buckets[bucketIdx];

              dirtyBuckets.remove(bucketIdx);
              evictionCandidates.remove(bucketIdx);
              buckets[bucketIdx] = null;

              listener.bucketOffLoaded(oldBucket.bucketKey);
              bucketStore.deleteBucket(bucketIdx);
              listener.bucketDeleted(oldBucket.bucketKey);
              if (recordStats) {
                updateCounter(CounterKeys.DELETED_BUCKETS, 1, false);
                updateCounter(CounterKeys.BUCKETS_IN_MEMORY, -1, false);
                numEventsRemoved += oldBucket.countOfUnwrittenEvents() + oldBucket.countOfWrittenEvents();
              }
              logger.debug("deleted bucket {} {}", oldBucket.bucketKey, bucketIdx);
            }
            else if(buckets[bucketIdx] == null) // May be due to eviction or due to operator crash
            {
              if(evictedBuckets.containsKey(bucketIdx) && evictedBuckets.get(bucketIdx) < requestedKey){
                bucketStore.deleteBucket(bucketIdx);
                logger.debug("deleted bucket positions for idx {}", bucketIdx);
              }
            }

            Map<Object, T> bucketDataInStore = bucketStore.fetchBucket(bucketIdx);

            //Delete the least recently used bucket in memory if the noOfBucketsInMemory threshold is reached.
            if (evictionCandidates.size() + 1 > noOfBucketsInMemory) {

              for (int anIndex : evictionCandidates) {
                bucketHeap.add(buckets[anIndex]);
              }
              int overFlow = evictionCandidates.size() + 1 - noOfBucketsInMemory;
              while (overFlow-- >= 0) {
                AbstractBucket<T> lruBucket = bucketHeap.poll();
                if (lruBucket == null) {
                  break;
                }
                // Do not evict buckets loaded in the current window
                if(requestedBuckets.contains(lruBucket.bucketKey)) {
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
                evictedBuckets.put(lruIdx, lruBucket.bucketKey);
                listener.bucketOffLoaded(lruBucket.bucketKey);
                if (recordStats) {
                  updateCounter(CounterKeys.EVICTED_BUCKETS, 1, false);
                  updateCounter(CounterKeys.BUCKETS_IN_MEMORY, -1, false);
                  numEventsRemoved += lruBucket.countOfUnwrittenEvents() + lruBucket.countOfWrittenEvents();
                }
                logger.debug("evicted bucket {} {}", lruBucket.bucketKey, lruIdx);
              }
            }

            AbstractBucket<T> bucket = buckets[bucketIdx];
            if (bucket == null || bucket.bucketKey != requestedKey) {
              bucket = createBucket(requestedKey);
              buckets[bucketIdx] = bucket;
              evictedBuckets.remove(bucketIdx);
            }
            bucket.setWrittenEvents(bucketDataInStore);
            evictionCandidates.add(bucketIdx);
            listener.bucketLoaded(bucket);
            if (recordStats) {
              updateCounter(CounterKeys.BUCKETS_IN_MEMORY, 1, false);
              updateCounter(CounterKeys.EVENTS_IN_MEMORY, bucketDataInStore.size() - numEventsRemoved, false);
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

  /**
   * Updates the counters and autometrics. Counters will be deprecated in the
   * future. Autometrics will be used thereafter.
   *
   * @param counter Counter key to update
   * @param count Count to be added
   * @param reset Whether to reset the counter before adding
   */
  protected void updateCounter(CounterKeys counter, long count, boolean reset)
  {
    if (bucketCounters != null) {
      if (reset) {
        bucketCounters.getCounter(counter).setValue(0);
      }
      bucketCounters.getCounter(counter).add(count);
    } else {
      switch (counter) {
        case BUCKETS_IN_MEMORY:
          if (reset) {
            bucketsInMemory = 0;
          }
          bucketsInMemory += count;
          break;
        case DELETED_BUCKETS:
          if (reset) {
            deletedBuckets = 0;
          }
          deletedBuckets += count;
          break;
        case EVENTS_COMMITTED_LAST_WINDOW:
          if (reset) {
            eventsCommittedLastWindow = 0;
          }
          eventsCommittedLastWindow += count;
          break;
        case EVENTS_IN_MEMORY:
          if (reset) {
            eventsInMemory = 0;
          }
          eventsInMemory += count;
          break;
        case EVICTED_BUCKETS:
          if (reset) {
            evictedBuckets = 0;
          }
          evictedBuckets += count;
          break;
        default:
          // Will never reach here
          break;
      }
    }
  }

  @Override
  public void setBucketStore(@Nonnull BucketStore<T> bucketStore)
  {
    this.bucketStore = bucketStore;
    bucketStore.setNoOfBuckets(noOfBuckets);
    bucketStore.setWriteEventKeysOnly(writeEventKeysOnly);
  }

  @Override
  public BucketStore<T> getBucketStore()
  {
    return bucketStore;
  }

  @Override
  public void startService(Listener<T> listener)
  {
    bucketStore.setup();
    logger.debug("bucket properties {}, {}, {}, {}", noOfBuckets, noOfBucketsInMemory, maxNoOfBucketsInMemory, millisPreventingBucketEviction);
    this.listener = Preconditions.checkNotNull(listener, "storageHandler");
    @SuppressWarnings("unchecked")
    AbstractBucket<T>[] freshBuckets = (AbstractBucket<T>[]) Array.newInstance(AbstractBucket.class, noOfBuckets);
    buckets = freshBuckets;
    //Create buckets for unwritten events which were check-pointed
    for (Map.Entry<Integer, AbstractBucket<T>> bucketEntry : dirtyBuckets.entrySet()) {
      buckets[bucketEntry.getKey()] = bucketEntry.getValue();
    }
    Thread eventServiceThread = new Thread(this, "BucketLoaderService");
    eventServiceThread.start();
  }

  @Override
  public AbstractBucket<T> getBucket(long bucketKey)
  {
    int bucketIdx = (int) (bucketKey % noOfBuckets);
    AbstractBucket<T> bucket = buckets[bucketIdx];
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

    AbstractBucket<T> bucket = buckets[bucketIdx];

    if (bucket == null || bucket.bucketKey != bucketKey) {
      bucket = createBucket(bucketKey);
      buckets[bucketIdx] = bucket;
      dirtyBuckets.put(bucketIdx, bucket);
    }
    else if (dirtyBuckets.get(bucketIdx) == null) {
      dirtyBuckets.put(bucketIdx, bucket);
    }

    bucket.addNewEvent(bucket.getEventKey(event), writeEventKeysOnly ? null : event);
    if (recordStats) {
      updateCounter(CounterKeys.EVENTS_IN_MEMORY, 1, false);
    }
  }

  @Override
  public void addEventToBucket(AbstractBucket<T> bucket, T event)
  {
    bucket.addNewEvent(bucket.getEventKey(event), writeEventKeysOnly ? null : event);
  }

  @Override
  public void endWindow(long window)
  {
    saveData(window, window);
  }

  @Override
  public void checkpointed(long window)
  {
  }

  @Override
  public void committed(long window)
  {
  }

  protected void saveData(long window, long id)
  {
    Map<Integer, Map<Object, T>> dataToStore = getDataToStore();
    long eventsCount = 0;
    for (Map<Object, T> events : dataToStore.values()) {
      eventsCount += events.size();
    }
    if (recordStats) {
      updateCounter(CounterKeys.EVENTS_COMMITTED_LAST_WINDOW, eventsCount, true);
    }
    if (!dataToStore.isEmpty()) {
      long start = System.currentTimeMillis();
      logger.debug("start store {}", window);
      storeData(window, id, dataToStore);
      logger.debug("end store {} num {} took {}", window, eventsCount, System.currentTimeMillis() - start);
    }
    dirtyBuckets.clear();
    committedWindow = window;
  }

  /**
   * Collects data from dirty buckets which needs to be stored into the persistent store.
   *
   * @return dataToStore The data which will be persisted in a new file pmn Hdfs
   */
  protected Map<Integer, Map<Object, T>> getDataToStore()
  {
    Map<Integer, Map<Object, T>> dataToStore = Maps.newHashMap();
    for (Map.Entry<Integer, AbstractBucket<T>> entry : dirtyBuckets.entrySet()) {
      AbstractBucket<T> bucket = entry.getValue();
      dataToStore.put(entry.getKey(), bucket.getUnwrittenEvents());
      if (collateFilesForBucket) {
        // Then collate all data together and write into a new file together
        if (bucket.getWrittenEventKeys() != null && !bucket.getWrittenEventKeys().isEmpty()) {
          dataToStore.put(entry.getKey(), bucket.getWrittenEvents());
          bucketsToDelete.add(entry.getKey()); // Record bucket to be deleted
        }
      }
      bucket.transferDataFromMemoryToStore();
      evictionCandidates.add(entry.getKey());
    }
    return dataToStore;
  }

  protected void storeData(long window, long id, Map<Integer, Map<Object, T>> dataToStore)
  {
    if (collateFilesForBucket) {
      for (int key : bucketsToDelete) {
        try {
          bucketStore.deleteBucket(key);
        } catch (Exception e) {
          throw new RuntimeException("Error deleting bucket index " + key, e);
        }
      }
    }
    try {
      bucketStore.storeBucketData(window, id, dataToStore);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  /*
   * Creates the specific bucket based on bucketKey provided.
   * @param bucketKey
   * @return Bucket
   */
  protected abstract AbstractBucket<T> createBucket(long bucketKey);

   @Override
  public void definePartitions(List<BucketManager<T>> oldManagers, Map<Integer, BucketManager<T>> partitionKeysToManagers, int partitionMask)
  {
    for (BucketManager<T> manager : oldManagers) {
      AbstractBucketManager<T> managerImpl = (AbstractBucketManager<T>) manager;

      for (Map.Entry<Integer, AbstractBucket<T>> bucketEntry : managerImpl.dirtyBuckets.entrySet()) {
        AbstractBucket<T> sourceBucket = bucketEntry.getValue();
        int sourceBucketIdx = bucketEntry.getKey();

        for (Map.Entry<Object, T> eventEntry : sourceBucket.getUnwrittenEvents().entrySet()) {
          int partition = eventEntry.getKey().hashCode() & partitionMask;
          AbstractBucketManager<T> newManagerImpl = (AbstractBucketManager<T>) partitionKeysToManagers.get(partition);

          AbstractBucket<T> destBucket = newManagerImpl.dirtyBuckets.get(sourceBucketIdx);
          if (destBucket == null) {
            destBucket = createBucket(sourceBucket.bucketKey);
            newManagerImpl.dirtyBuckets.put(sourceBucketIdx, destBucket);
          }
          destBucket.addNewEvent(eventEntry.getKey(), eventEntry.getValue());
        }
      }
    }
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

    if(!(o instanceof AbstractBucketManager)) {
      return false;
    }

    @SuppressWarnings("unchecked")
    AbstractBucketManager<T> that = (AbstractBucketManager<T>)o;

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

  @Override
  public AbstractBucketManager<T> clone() throws CloneNotSupportedException
  {
    @SuppressWarnings("unchecked")
    AbstractBucketManager<T> clone = (AbstractBucketManager<T>)super.clone();
    clone.setBucketStore(clone.getBucketStore().clone());
    return clone;
  }

  // Getters for Bucket Metrics
  public long getDeletedBuckets()
  {
    return deletedBuckets;
  }

  public long getEvictedBuckets()
  {
    return evictedBuckets;
  }

  public long getEventsInMemory()
  {
    return eventsInMemory;
  }

  public long getEventsCommittedLastWindow()
  {
    return eventsCommittedLastWindow;
  }

  public long getEndOfBuckets()
  {
    return endOfBuckets;
  }

  public long getStartOfBuckets()
  {
    return startOfBuckets;
  }

  // Setters for Bucket Metrics
  public void setDeletedBuckets(long deletedBuckets)
  {
    this.deletedBuckets = deletedBuckets;
  }

  public void setBucketsInMemory(long bucketsInMemory)
  {
    this.bucketsInMemory = bucketsInMemory;
  }

  public void setEvictedBuckets(long evictedBuckets)
  {
    this.evictedBuckets = evictedBuckets;
  }

  public void setEventsInMemory(long eventsInMemory)
  {
    this.eventsInMemory = eventsInMemory;
  }

  public void setEventsCommittedLastWindow(long eventsCommittedLastWindow)
  {
    this.eventsCommittedLastWindow = eventsCommittedLastWindow;
  }

  public void setEndOfBuckets(long endOfBuckets)
  {
    this.endOfBuckets = endOfBuckets;
  }

  public void setStartOfBuckets(long startOfBuckets)
  {
    this.startOfBuckets = startOfBuckets;
  }

  public boolean isCollateFilesForBucket()
  {
    return collateFilesForBucket;
  }

  /**
   * Sets whether entire bucket data must be persisted together in a new file.
   * When this is not set, only the unwritten part of the bucket is persisted.
   *
   * @param collateFilesForBucket
   */
  public void setCollateFilesForBucket(boolean collateFilesForBucket)
  {
    this.collateFilesForBucket = collateFilesForBucket;
  }

  private static transient final Logger logger = LoggerFactory.getLogger(AbstractBucketManager.class);
}
