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

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.mutable.MutableLong;

import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.lib.counters.BasicCounters;

/**
 * <p>
 * Bucket manager creates and manages {@link Bucket}s for an Operator.<br/>
 * Only a limited number of buckets can be held in memory at a particular time. The manager is responsible for loading
 * of a bucket when it is requested by the operator and un-loading least recently used buckets to optimize memory usage.
 * </p>
 *
 * <p>
 * Loading of buckets and saving new bucket events to storage is triggered by the Operator. Typically an Operator would:
 * <ol>
 * <li>fetch the bucket key of an event by calling {@link #getBucketKeyFor(Bucketable)}.</li>
 * <li>invoke {@link #getBucket(long)}. If this returns null or events from disk are not loaded then the operator can ask
 * the manager to load the bucket by calling {@link BucketManager#loadBucketData(long)}. This is not a blocking call.<br/>
 * </li>
 * <li>
 * Once the manager loads a bucket, it informs {@link BucketManager.Listener} by calling {@link BucketManager.Listener#bucketLoaded(Bucket)}.<br/>
 * If there were some buckets that were off-loaded during the process {@link BucketManager.Listener#bucketOffLoaded(long)}
 * callback is triggered.
 * </li>
 * <li>
 * The operator could then add new events to a bucket by invoking {@link #newEvent(long, Bucketable)}. These events are
 * maintained in a check-pointed state.
 * </li>
 * <li>
 * To ensure that at any given point of time all the load requests are completed, the operator can trigger {@link #blockUntilAllRequestsServiced()}.
 * </li>
 * <li>
 * The operator triggers {@link #endWindow(long)} which tells the manager to persist un-written data.
 * </li>
 * </ol>
 * </p>
 *
 * @param <T> event type
 * @since 0.9.4
 */
public interface BucketManager<T> extends Cloneable, CheckpointListener
{
  void setBucketStore(@Nonnull BucketStore<T> bucketStore);

  BucketStore<T> getBucketStore();

  /**
   * Starts the service.
   *
   * @param listener {@link Listener} which will be informed when bucket are loaded and off-loaded.
   */
  void startService(Listener<T> listener);

  /**
   * Shuts down the service.
   */
  void shutdownService();

  /**
   * Calculates the bucket key of an event.<br/>
   * -ve values indicate invalid event.
   *
   * @param event event
   * @return bucket key for event.
   */
  long getBucketKeyFor(T event);

  /**
   * <p>
   * Returns the bucket in memory corresponding to a bucket key.
   * If the bucket is not created yet, it will return null.
   * A bucket is created:
   * <ul>
   * <li> When the operator requests to load a bucket from store.</li>
   * <li>In {@link #startService(Listener)} if there were un-written bucket events which were
   * check-pointed by the engine.</li>
   * </ul>
   * </p>
   *
   * @param bucketKey key of the bucket.
   * @return bucket; null if the bucket is not yet created.
   */
  @Nullable
  AbstractBucket<T> getBucket(long bucketKey);

  /**
   * Loads the events belonging to the bucket from the store. <br/>
   * The events loaded belong to the windows less than the current window.
   *
   * @param bucketKey key of the bucket
   */
  void loadBucketData(long bucketKey);

  /**
   * Adds the event to the un-written section of the bucket corresponding to the bucket key.
   *
   * @param bucketKey key of the bucket.
   * @param event     new event.
   */
  void newEvent(long bucketKey, T event);

  /**
   * Adds the event to the un-written section of the bucket to the given bucket.
   *
   * @param bucket
   * @param event     new event.
   */
  void addEventToBucket(AbstractBucket<T> bucket, T event);

  /**
   * Does end window operations which includes tracking the committed window and
   * persisting all un-written events in the store.
   *
   * @param window window number.
   */
  void endWindow(long window);

  /**
   * Blocks the calling thread until all the load requests of this window have been serviced.
   *
   * @throws InterruptedException
   */
  void blockUntilAllRequestsServiced() throws InterruptedException;

  /**
   * Constructs a new {@link BucketManager} with only the settings and not the data.
   *
   * @return newly created manager without any data.
   * @throws java.lang.CloneNotSupportedException
   */
  BucketManager<T> clone() throws CloneNotSupportedException;

  /*
   * This method has been deprecated.Use clone instead.
   */
  @Deprecated
  BucketManager<T> cloneWithProperties();

  void setBucketCounters(@Nonnull BasicCounters<MutableLong> stats);

  /**
   * Collects the un-written events of all the old managers and distributes the data to the new managers.<br/>
   * The partition to which an event belongs to depends on the event key.
   * <pre><code>partition = eventKey.hashCode() & partitionMask</code></pre>
   *
   * @param oldManagers             {@link BucketManager}s of all old partitions.
   * @param partitionKeysToManagers mapping of partition keys to {@link BucketManager}s of new partitions.
   * @param partitionMask           partition mask to find which partition an event belongs to.
   */
  void definePartitions(List<BucketManager<T>> oldManagers, Map<Integer, BucketManager<T>> partitionKeysToManagers, int partitionMask);

  /**
   * Callback interface for {@link BucketManager} for load and off-load operations.
   *
   * @param <T> Type of the values which can be bucketed.
   */
  public static interface Listener<T>
  {
    /**
     * Invoked when a bucket is loaded from store.
     *
     * @param loadedBucket bucket that was loaded.
     */
    void bucketLoaded(AbstractBucket<T> loadedBucket);

    /**
     * Invoked when a bucket is removed from memory.<br/>
     * If the listener has cached a bucket it should use this callback to remove it from the cache.
     *
     * @param bucketKey key of the bucket which was off-loaded.
     */
    void bucketOffLoaded(long bucketKey);

    /**
     * Invoked when a bucket is deleted.<br/>
     * This will be called only when the bucket is deleted and no longer needed.
     *
     * @param bucketKey key of the bucket which was deleted.
     */
    void bucketDeleted(long bucketKey);
  }

  public static enum CounterKeys
  {

    BUCKETS_IN_MEMORY, EVICTED_BUCKETS, DELETED_BUCKETS, EVENTS_COMMITTED_LAST_WINDOW,
    EVENTS_IN_MEMORY
  }
}