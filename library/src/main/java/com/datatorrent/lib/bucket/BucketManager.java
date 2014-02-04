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

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;

import com.datatorrent.api.Operator;

import com.datatorrent.common.util.DTThrowable;

/**
 * <p>
 * Creates and manages {@link Bucket}s for an Operator.<br/>
 * Only a limited number of buckets can be held in memory at a particular time. The manager is responsible for loading
 * of a bucket when it is requested by the operator and un-loading least recently used buckets to optimize memory usage.
 * </p>
 *
 * <p>
 * Configurable properties of the BucketManager.<br/>
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
 * <p>
 * The manager has a dedicated thread running to fetch buckets from {@link BucketStore}. This thread operates on a queue of
 * {@link BucketManager.LoadCommand}.<br/>
 * Loading of buckets and saving new bucket events to storage is triggered by the Operator. Typically an Operator would:
 * <ol>
 * <li>invoke {@link #getBucket(long)}.</li>
 * <li>
 * If the above call returns <code>null</code> then issue {@link BucketManager.LoadCommand} to the manager
 * by calling {@link #loadBucketData(LoadCommand)}. This is not a blocking call.
 * </li>
 * <li>
 * When the manager has loaded an event it informs {@link BucketManager.Listener} that a bucket is loaded by
 * calling {@link BucketManager.Listener#bucketLoaded(long)}.<br/>
 * If there were some buckets that were off-loaded during the process {@link BucketManager.Listener#bucketOffLoaded(long)}
 * callback is triggered.
 * </li>
 * <li>
 * The operator could then add new events to a bucket by invoking {@link #newEvent(long, BucketEvent)}. These events are
 * maintained in a check-pointed state.
 * </li>
 * <li>
 * The operator needs to ensure that the manager services all the load request in the current window which is achieved by
 * calling {@link #blockUntilAllBucketsLoaded()} in the {@link Operator#endWindow()}.
 * </li>
 * <li>
 * The operator would then process the events which were waiting for the bucket to load.
 * </li>
 * <li>
 * The operator triggers {@link #endWindow(long)} which tells the manager to persist un-written data.
 * </li>
 * </ol>
 * </p>
 * @param <T>
 */
public class BucketManager<T extends BucketEvent> implements Runnable
{
  //Check-pointed
  @Min(1)
  protected int noOfBuckets;
  @Min(1)
  private int noOfBucketsInMemory;
  @Min(1)
  private int maxNoOfBucketsInMemory;
  @Min(0)
  private long millisPreventingBucketEviction;
  private boolean writeEventKeysOnly;
  @Nonnull
  private final Map<Long, Map<Object, T>> unwrittenBucketEvents;
  private long committedWindow;

  //Not check-pointed
  //Indexed by bucketKey keys.
  protected transient Bucket<T>[] buckets;
  @Nonnull
  protected transient BucketStore<T> bucketStore;
  @Nonnull
  protected transient Set<Long> knownBucketKeys;
  protected transient Listener listener;
  @Nonnull
  private transient final BlockingQueue<LoadCommand> eventQueue;
  @Nonnull
  private transient final Thread eventServiceThread;
  private transient volatile boolean running;
  @Nonnull
  private transient final Lock lock;
  @Nonnull
  private transient final MinMaxPriorityQueue<Bucket<T>> bucketHeap;

  BucketManager()
  {
    eventQueue = new LinkedBlockingQueue<LoadCommand>();
    eventServiceThread = new Thread(this, "BucketManager");
    knownBucketKeys = Sets.newHashSet();
    unwrittenBucketEvents = Maps.newHashMap();
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
    maxNoOfBucketsInMemory = this.noOfBucketsInMemory + 10;
    committedWindow = -1;
  }

  /**
   * Constructs a bucket storage manager with the given parameters and
   * <pre>
   *   <code>{@link #maxNoOfBucketsInMemory} = {@link #noOfBucketsInMemory} + 10</code>
   * </pre>
   *
   * @param writeEventKeysOnly             true for keeping only event keys in memory and store; false otherwise.
   * @param noOfBuckets                    total no. of buckets.
   * @param noOfBucketsInMemory            number of buckets in memory.
   * @param millisPreventingBucketEviction duration in millis that prevent a bucket from being off-loaded.
   */
  public BucketManager(boolean writeEventKeysOnly, int noOfBuckets,
                       int noOfBucketsInMemory, long millisPreventingBucketEviction)
  {
    this();
    this.writeEventKeysOnly = writeEventKeysOnly;
    this.noOfBuckets = noOfBuckets;
    this.noOfBucketsInMemory = noOfBucketsInMemory;
    this.maxNoOfBucketsInMemory = noOfBucketsInMemory + 10;
    this.millisPreventingBucketEviction = millisPreventingBucketEviction;
    logger.debug("created {} buckets", this.noOfBuckets);
  }

  /**
   * Constructs a bucket storage manager with the given parameters.
   *
   * @param writeEventKeysOnly             true for keeping only event keys in memory and store; false otherwise.
   * @param noOfBuckets                    total no. of buckets.
   * @param noOfBucketsInMemory            number of buckets in memory.
   * @param millisPreventingBucketEviction duration in millis that prevent a bucket from being off-loaded.
   * @param maxNoOfBucketsInMemory         hard limit on no. of buckets in memory.
   */
  public BucketManager(boolean writeEventKeysOnly, int noOfBuckets, int noOfBucketsInMemory,
                       long millisPreventingBucketEviction, int maxNoOfBucketsInMemory)
  {
    this(writeEventKeysOnly, noOfBuckets, noOfBucketsInMemory, millisPreventingBucketEviction);
    this.maxNoOfBucketsInMemory = maxNoOfBucketsInMemory;
  }

  @Override
  public void run()
  {
    running = true;
    try {
      while (running) {
        LoadCommand item = eventQueue.poll(1, TimeUnit.SECONDS);
        if (item != null) {
          if (item.bucketKey == -1) {
            synchronized (lock) {
              lock.notifyAll();
            }
          }
          else {
            int bucketIdx = (int) item.bucketKey % noOfBuckets;
            Map<Object, T> bucketDataInStore = bucketStore.fetchBucket(bucketIdx);

            if (buckets[bucketIdx] != null && buckets[bucketIdx].bucketKey != item.bucketKey) {
              //Delete the old bucket in memory at that index.
              Bucket<T> oldBucket = buckets[bucketIdx];

              unwrittenBucketEvents.remove(oldBucket.bucketKey);
              knownBucketKeys.remove(oldBucket.bucketKey);
              buckets[bucketIdx] = null;

              listener.bucketOffLoaded(oldBucket.bucketKey);
              bucketStore.deleteBucket(bucketIdx);
              //logger.debug("deleted bucket {}", oldBucket.bucketKey);
            }

            //Delete the least recently used bucket in memory if the noOfBucketsInMemory threshold is reached.
            if (knownBucketKeys.size() + 1 > noOfBucketsInMemory) {
              for (long key : knownBucketKeys) {
                int idx = (int) key % noOfBuckets;
                bucketHeap.add(buckets[idx]);
              }
              int overFlow = knownBucketKeys.size() + 1 - noOfBucketsInMemory;
              while (overFlow >= 0) {
                Bucket<T> lruBucket = bucketHeap.poll();
                if (lruBucket == null) {
                  break;
                }
                if (unwrittenBucketEvents.containsKey(lruBucket.bucketKey)) {
                  break;
                }
                if (((System.currentTimeMillis() - lruBucket.lastUpdateTime()) < millisPreventingBucketEviction) &&
                  ((knownBucketKeys.size() + 1) <= maxNoOfBucketsInMemory)) {
                  break;
                }
                knownBucketKeys.remove(lruBucket.bucketKey);
                int lruIdx = (int) lruBucket.bucketKey % noOfBuckets;
                buckets[lruIdx] = null;
                listener.bucketOffLoaded(lruBucket.bucketKey);
                overFlow--;
                //logger.debug("evicted bucket {}", lruBucket.bucketKey);
              }
            }

            Bucket<T> bucket = buckets[bucketIdx];
            if (bucket == null) {
              bucket = new Bucket<T>(item.bucketKey);
              buckets[bucketIdx] = bucket;
            }
            bucket.setWrittenEvents(bucketDataInStore);
            bucket.updateAccessTime();
            knownBucketKeys.add(item.bucketKey);
            listener.bucketLoaded(item.bucketKey);

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
   * Starts the service.
   *
   * @param bucketStore a {@link BucketStore} to/from which data is saved/loaded.
   * @param listener    {@link Listener} which will be informed when bucket are loaded and off-loaded.
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public void startService(BucketStore<T> bucketStore, Listener listener) throws Exception
  {
    buckets = (Bucket<T>[]) Array.newInstance(Bucket.class, noOfBuckets);
    this.listener = Preconditions.checkNotNull(listener, "storageHandler");
    this.bucketStore = Preconditions.checkNotNull(bucketStore, "bucket store");
    this.bucketStore.setup(noOfBuckets, committedWindow, writeEventKeysOnly);

    //Create buckets for unwritten events which were check-pointed
    for (long bucketKey : unwrittenBucketEvents.keySet()) {
      Bucket<T> startBucket = new Bucket<T>(bucketKey);
      startBucket.setUnwrittenEvents(unwrittenBucketEvents.get(bucketKey));

      int idx = (int) bucketKey % noOfBuckets;
      buckets[idx] = startBucket;
      knownBucketKeys.add(bucketKey);
    }

    eventServiceThread.start();
  }

  /**
   * Shuts down the service.
   */
  public void shutdownService()
  {
    running = false;
    bucketStore.teardown();
  }

  /**
   * <p>
   * Returns the bucket in memory corresponding to a bucket key.
   * If the bucket is not created yet, it will return null.</br>
   * A bucket is created:
   * <ul>
   * <li> When the operator requests to load a bucket from store.</li>
   * <li>In {@link #startService(BucketStore, Listener)} if there were un-written bucket events which were
   * check-pointed by the engine.</li>
   * </ul>
   * </p>
   *
   * @param bucketKey key of the bucket.
   * @return bucket; null if the bucket is not yet created.
   */
  @Nullable
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

  /**
   * Adds the event to the un-written section of the bucket corresponding to the bucket key.
   *
   * @param bucketKey key of the bucket.
   * @param event     new event.
   */
  public void newEvent(long bucketKey, T event)
  {
    Map<Object, T> unwritten = unwrittenBucketEvents.get(bucketKey);
    if (unwritten != null) {
      if (writeEventKeysOnly) {
        unwritten.put(event.getEventKey(), null);
      }
      else {
        unwritten.put(event.getEventKey(), event);
      }

      return;
    }
    unwritten = Maps.newHashMap();
    if (writeEventKeysOnly) {
      unwritten.put(event.getEventKey(), null);
    }
    else {
      unwritten.put(event.getEventKey(), event);
    }
    unwrittenBucketEvents.put(bucketKey, unwritten);
    buckets[(int) (bucketKey % noOfBuckets)].setUnwrittenEvents(unwritten);
  }

  /**
   * Does end window operations which includes tracking the committed window and
   * persisting all un-written events in the store.
   *
   * @param window window number.
   * @throws Exception
   */
  public void endWindow(long window) throws Exception
  {
    for (Iterator<Map.Entry<Long, Map<Object, T>>> bucketIterator = unwrittenBucketEvents.entrySet().iterator();
         bucketIterator.hasNext(); ) {
      Map.Entry<Long, Map<Object, T>> entry = bucketIterator.next();
      int bucketIdx = (int) (entry.getKey() % noOfBuckets);
      Bucket<T> bucket = buckets[bucketIdx];
      bucket.transferDataFromMemoryToStore();
      bucketStore.storeBucketData(bucketIdx, window, entry.getValue());
      bucketIterator.remove();
    }
    committedWindow = window;
  }

  /**
   * Blocks the calling thread until all the load requests of this window have been serviced.
   * @throws InterruptedException
   */
  public void blockUntilAllBucketsLoaded() throws InterruptedException
  {
    synchronized (lock) {
      eventQueue.offer(LoadCommand.queryCommand);
      lock.wait();
    }
    Preconditions.checkArgument(eventQueue.size() == 0, eventQueue);
  }

  /**
   * Loads the events belonging to the bucket from the store. <br/>
   * The events loaded belong to the windows less than the current window.
   *
   * @param command the command to load the bucket.
   */
  public void loadBucketData(LoadCommand command)
  {
    eventQueue.offer(command);
  }

  /**
   * Collects the un-written events of all the old managers and distributes the data to the new managers.<br/>
   * The partition to which an event belongs to depends on the event key.
   * <pre><code>partition = eventKey.hashCode() & partitionMask</code></pre>
   *
   * @param oldManagers             {@link BucketManager}s of all old partitions.
   * @param partitionKeysToManagers mapping of partition keys to {@link BucketManager}s of new partitions.
   * @param partitionMask           partition mask to find which partition an event belongs to.
   */
  public void definePartitions(List<BucketManager<T>> oldManagers, Map<Integer, BucketManager<T>> partitionKeysToManagers, int partitionMask)
  {
    for (BucketManager<T> manager : oldManagers) {
      for (Map.Entry<Long, Map<Object, T>> unwrittenBucket : manager.unwrittenBucketEvents.entrySet()) {
        for (Object unwrittenEventKey : unwrittenBucket.getValue().keySet()) {
          int partition = unwrittenEventKey.hashCode() & partitionMask;

          Map<Object, T> dest = partitionKeysToManagers.get(partition).unwrittenBucketEvents.get(unwrittenBucket.getKey());
          if (dest == null) {
            dest = Maps.newHashMap();
            partitionKeysToManagers.get(partition).unwrittenBucketEvents.put(unwrittenBucket.getKey(), dest);
          }
          if (writeEventKeysOnly) {
            dest.put(unwrittenEventKey, null);
          }
          else {
            dest.put(unwrittenEventKey, unwrittenBucket.getValue().get(unwrittenEventKey));
          }
        }
      }
    }
  }

  /**
   * Constructs a new {@link BucketManager} with only the settings and not the data.
   *
   * @return newly created manager without any data.
   */
  public BucketManager<T> cloneWithProperties()
  {
    BucketManager<T> clone = new BucketManager<T>(writeEventKeysOnly, noOfBuckets, noOfBucketsInMemory,
      millisPreventingBucketEviction, maxNoOfBucketsInMemory);
    clone.committedWindow = committedWindow;
    return clone;
  }

  /**
   * Command issued to the {@link BucketManager} to load a bucket from persistent store.<br/>
   * Bucket key should be positive. Negative values are reserved by the storage manager.
   */
  public static class LoadCommand
  {
    private static LoadCommand queryCommand = new LoadCommand(-1);
    final long bucketKey;

    public LoadCommand(long bucketKey)
    {
      this.bucketKey = bucketKey;
    }

    @Override
    public String toString()
    {
      return "LoadCommand{" + bucketKey + '}';
    }
  }

  /**
   * Callback interface for {@link BucketManager} load and off-load operations.
   */
  public static interface Listener
  {
    /**
     * Invoked when a bucket is loaded from store.
     *
     * @param bucketKey bucket key.
     */
    void bucketLoaded(long bucketKey);

    /**
     * Invoked when a bucket is removed from memory.<br/>
     * If the listener has cached a bucket it should use this callback to remove it from the cache.
     *
     * @param bucketKey key of the bucket which was off-loaded.
     */
    void bucketOffLoaded(long bucketKey);

  }

  @SuppressWarnings("ClassMayBeInterface")
  private class Lock
  {
  }

  private static transient final Logger logger = LoggerFactory.getLogger(BucketManager.class);
}
