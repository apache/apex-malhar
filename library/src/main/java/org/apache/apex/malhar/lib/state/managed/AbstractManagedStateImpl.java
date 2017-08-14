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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fileaccess.FileAccess;
import org.apache.apex.malhar.lib.fileaccess.TFileImpl;
import org.apache.apex.malhar.lib.util.comparator.SliceComparator;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.Futures;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.netlet.util.Slice;

/**
 * An abstract implementation of managed state.<br/>
 *
 * The important sub-components here are:
 * <ol>
 *   <li>
 *     {@link #checkpointManager}: writes incremental checkpoints in window files and transfers data from window
 *     files to bucket files.
 *   </li>
 *   <li>
 *     {@link #bucketsFileSystem}: manages writing/reading from all the buckets. A bucket on disk is further sub-divided
 *     into time-buckets. This abstracts out updating time-buckets and meta files and reading from them.
 *   </li>
 *   <li>
 *     {@link #timeBucketAssigner}: assigns time-buckets to keys and manages the time boundaries.
 *   </li>
 *   <li>
 *     {@link #stateTracker}: tracks the size of data in memory and requests buckets to free memory when enough memory
 *     is not available.
 *   </li>
 *   <li>
 *     {@link #fileAccess}: plug-able file system abstraction.
 *   </li>
 * </ol>
 * <p/>
 * <b>Differences between different concrete implementations of {@link AbstractManagedStateImpl}</b>
 * <table>
 *   <tr>
 *     <td></td>
 *     <td>{@link ManagedStateImpl}</td>
 *     <td>{@link ManagedTimeStateImpl}</td>
 *     <td>{@link ManagedTimeUnifiedStateImpl}</td>
 *   </tr>
 *   <tr>
 *     <td>Main buckets</td>
 *     <td>identified by unique adhoc long ids that the user provides with the key.</td>
 *     <td>same as ManagedStateImpl.</td>
 *     <td>user doesn't provide bucket ids and instead just provides time. Time is used to derive the time buckets
 *     and these are the main buckets.</td>
 *   </tr>
 *   <tr>
 *     <td>Data on disk: data in buckets is persisted on disk with each bucket data further divided into
 *     time-buckets, i.e., {base_path}/{bucketId}/{time-bucket id}</td>
 *     <td>time-bucket is computed using the system time corresponding to the application window.</td>
 *     <td>time-bucket is derived from the user provided time.</td>
 *     <td>time-bucket is derived from the user provided time.
 *     In this implementation operator id is used to isolate data of different partitions on disk, i.e.,
 *     {base_path}/{operatorId}/{time-bucket id}</td>
 *   </tr>
 *   <tr>
 *     <td>Bucket partitioning</td>
 *     <td>bucket belongs to just one partition. Multiple partitions cannot write to the same bucket.</td>
 *     <td>same as ManagedStateImpl.</td>
 *     <td>multiple partitions can be working with the same time-bucket since time-bucket is derived from time.
 *     This works because on disk each partition's data is segregated by the operator id.</td>
 *   </tr>
 *   <tr>
 *     <td>Dynamic partitioning</td>
 *     <td>can support dynamic partitioning by pre-allocating buckets.</td>
 *     <td>same as ManagedStateImpl.</td>
 *     <td>will not be able to support dynamic partitioning efficiently.</td>
 *   </tr>
 * </table>
 *
 *
 * @since 3.4.0
 */
public abstract class AbstractManagedStateImpl
    implements ManagedState, Component<OperatorContext>, Operator.CheckpointNotificationListener, ManagedStateContext,
    TimeBucketAssigner.PurgeListener, BucketProvider
{
  private long maxMemorySize;

  protected long numBuckets;

  @NotNull
  private FileAccess fileAccess = new TFileImpl.DTFileImpl();
  @NotNull
  protected TimeBucketAssigner timeBucketAssigner;

  protected Map<Long, Bucket> buckets;

  @Min(1)
  private int numReaders = 1;
  @NotNull
  protected transient ExecutorService readerService;

  @NotNull
  private IncrementalCheckpointManager checkpointManager = new IncrementalCheckpointManager();

  @NotNull
  protected BucketsFileSystem bucketsFileSystem = new BucketsFileSystem();

  protected transient OperatorContext operatorContext;

  @NotNull
  protected Comparator<Slice> keyComparator = new SliceComparator();

  protected final transient AtomicReference<Throwable> throwable = new AtomicReference<>();

  @NotNull
  @FieldSerializer.Bind(JavaSerializer.class)
  private Duration checkStateSizeInterval = Duration.millis(60000);

  @FieldSerializer.Bind(JavaSerializer.class)
  private Duration durationPreventingFreeingSpace;

  private transient StateTracker stateTracker = new StateTracker();

  //accessible to StateTracker
  final transient Object commitLock = new Object();

  protected final transient ListMultimap<Long, ValueFetchTask> tasksPerBucketId =
      Multimaps.synchronizedListMultimap(ArrayListMultimap.<Long, ValueFetchTask>create());

  @Override
  public void setup(OperatorContext context)
  {
    operatorContext = context;
    fileAccess.init();

    if (timeBucketAssigner == null) {
      // set default time bucket assigner
      MovingBoundaryTimeBucketAssigner movingBoundaryTimeBucketAssigner = new MovingBoundaryTimeBucketAssigner();
      setTimeBucketAssigner(movingBoundaryTimeBucketAssigner);
    }
    timeBucketAssigner.setPurgeListener(this);

    //setup all the managed state components
    timeBucketAssigner.setup(this);
    checkpointManager.setup(this);
    bucketsFileSystem.setup(this);

    if (buckets == null) {
      //create buckets map only once at start if it is not created.
      numBuckets = getNumBuckets();
      buckets = new HashMap<>();
    }
    for (Bucket bucket : buckets.values()) {
      if (bucket != null) {
        bucket.setup(this);
      }
    }

    stateTracker.setup(this);
    long activationWindow = context.getValue(OperatorContext.ACTIVATION_WINDOW_ID);

    if (activationWindow != Stateless.WINDOW_ID) {
      //All the wal files with windows <= activationWindow are loaded and kept separately as recovered data.
      try {

        Map<Long, Object> statePerWindow = checkpointManager.retrieveAllWindows();
        for (Map.Entry<Long, Object> stateEntry : statePerWindow.entrySet()) {
          Preconditions.checkArgument(stateEntry.getKey() <= activationWindow,
              stateEntry.getKey() + " greater than " + activationWindow);
          @SuppressWarnings("unchecked")
          Map<Long, Map<Slice, Bucket.BucketedValue>> state = (Map<Long, Map<Slice, Bucket.BucketedValue>>)
              stateEntry.getValue();
          if (state != null && !state.isEmpty()) {
            for (Map.Entry<Long, Map<Slice, Bucket.BucketedValue>> bucketEntry : state.entrySet()) {
              long bucketIdx = prepareBucket(bucketEntry.getKey());
              buckets.get(bucketIdx).recoveredData(stateEntry.getKey(), bucketEntry.getValue());
            }
          }
          // Skip write to WAL during recovery during replay from WAL.
          // Data only needs to be transferred to bucket data files.
          checkpointManager.save(state, stateEntry.getKey(), true /*skipWritingToWindowFile*/);
        }
      } catch (IOException e) {
        throw new RuntimeException("recovering", e);
      }
    }

    readerService = Executors.newFixedThreadPool(numReaders, new NameableThreadFactory("managedStateReaders"));
  }

  /**
   * Gets the number of buckets which is required during setup to create the array of buckets.<br/>
   * {@link ManagedTimeStateImpl} provides num of buckets which is injected using a property.<br/>
   * {@link ManagedTimeUnifiedStateImpl} provides num of buckets which are calculated based on time settings.
   *
   * @return number of buckets.
   */
  public abstract long getNumBuckets();

  public void beginWindow(long windowId)
  {
    if (throwable.get() != null) {
      Throwables.propagate(throwable.get());
    }
  }


  /**
   * Prepares the bucket and returns its index.
   * @param bucketId bucket key
   * @return bucket index
   */
  protected long prepareBucket(long bucketId)
  {
    stateTracker.bucketAccessed(bucketId);
    long bucketIdx = getBucketIdx(bucketId);

    Bucket bucket = buckets.get(bucketIdx);
    if (bucket == null) {
      //bucket is not in memory
      bucket = newBucket(bucketId);
      bucket.setup(this);
      buckets.put(bucketIdx, bucket);
    } else if (bucket.getBucketId() != bucketId) {
      handleBucketConflict(bucketIdx, bucketId);
    }
    return bucketIdx;
  }

  protected void putInBucket(long bucketId, long timeBucket, @NotNull Slice key, @NotNull Slice value)
  {
    Preconditions.checkNotNull(key, "key");
    Preconditions.checkNotNull(value, "value");
    if (timeBucket != -1) {
      //time bucket is invalid data is not stored
      long bucketIdx = prepareBucket(bucketId);
      //synchronization on a bucket isn't required for put because the event is added to flash which is
      //a concurrent map. The assumption here is that the calls to put & get(sync/async) are being made synchronously by
      //a single thread (operator thread). The get(sync/async) always checks memory first synchronously.
      //If the key is not in the memory, then the async get will uses other reader threads which will fetch it from
      //the files.
      buckets.get(bucketIdx).put(key, timeBucket, value);
    }
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  protected Slice getValueFromBucketSync(long bucketId, long timeBucket, @NotNull Slice key)
  {
    Preconditions.checkNotNull(key, "key");
    long bucketIdx = prepareBucket(bucketId);
    Bucket bucket = buckets.get(bucketIdx);
    synchronized (bucket) {
      return bucket.get(key, timeBucket, Bucket.ReadSource.ALL);
    }
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  protected Future<Slice> getValueFromBucketAsync(long bucketId, long timeBucket, @NotNull Slice key)
  {
    Preconditions.checkNotNull(key, "key");
    long bucketIdx = prepareBucket(bucketId);
    Bucket bucket = buckets.get(bucketIdx);
    synchronized (bucket) {
      Slice cachedVal = bucket.get(key, timeBucket, Bucket.ReadSource.MEMORY);
      if (cachedVal != null) {
        return Futures.immediateFuture(cachedVal);
      }
      ValueFetchTask valueFetchTask = new ValueFetchTask(bucket, key, timeBucket, this);
      tasksPerBucketId.put(bucket.getBucketId(), valueFetchTask);
      return readerService.submit(valueFetchTask);
    }
  }

  protected void handleBucketConflict(long bucketIdx, long newBucketId)
  {
    throw new IllegalArgumentException("bucket conflict " + buckets.get(bucketIdx).getBucketId() + " " + newBucketId);
  }

  protected long getBucketIdx(long bucketId)
  {
    return Math.abs(bucketId % numBuckets);
  }

  @Override
  public Bucket getBucket(long bucketId)
  {
    return buckets.get(getBucketIdx(bucketId));
  }

  @Override
  public Bucket ensureBucket(long bucketId)
  {
    Bucket b = getBucket(bucketId);
    if (b == null) {
      b = newBucket(bucketId);
      b.setup(this);
      buckets.put(getBucketIdx(bucketId), b);
    }
    return b;
  }

  protected Bucket newBucket(long bucketId)
  {
    return new Bucket.DefaultBucket(bucketId);
  }

  public void endWindow()
  {
    timeBucketAssigner.endWindow();
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public void beforeCheckpoint(long windowId)
  {
    Map<Long, Map<Slice, Bucket.BucketedValue>> flashData = Maps.newHashMap();

    for (Bucket bucket : buckets.values()) {
      if (bucket != null) {
        synchronized (bucket) {
          Map<Slice, Bucket.BucketedValue> flashDataForBucket = bucket.checkpoint(windowId);
          if (!flashDataForBucket.isEmpty()) {
            flashData.put(bucket.getBucketId(), flashDataForBucket);
          }
        }
      }
    }
    if (!flashData.isEmpty()) {
      try {
        // write incremental state to WAL (skipWrite=false) before the checkpoint
        checkpointManager.save(flashData, windowId, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public void committed(long windowId)
  {
    synchronized (commitLock) {
      try {
        for (Bucket bucket : buckets.values()) {
          if (bucket != null) {
            synchronized (bucket) {
              bucket.committed(windowId);
            }
          }
        }
        checkpointManager.committed(windowId);
      } catch (IOException e) {
        throw new RuntimeException("committing " + windowId, e);
      }
    }
  }

  /**
   * get the memory usage for each bucket
   * @return The map of bucket id to memory size used by the bucket
   */
  public Map<Long, Long> getBucketMemoryUsage()
  {
    Map<Long, Long> bucketToSize = Maps.newHashMap();
    for (Bucket bucket : buckets.values()) {
      if (bucket == null) {
        continue;
      }
      bucketToSize.put(bucket.getBucketId(), bucket.getKeyStream().size() + bucket.getValueStream().size());
    }
    return bucketToSize;
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public void teardown()
  {
    checkpointManager.teardown();
    bucketsFileSystem.teardown();
    timeBucketAssigner.teardown();
    readerService.shutdownNow();
    for (Bucket bucket : buckets.values()) {
      if (bucket != null) {
        synchronized (bucket) {
          bucket.teardown();
        }
      }
    }
    stateTracker.teardown();
  }

  @Override
  public void purgeTimeBucketsLessThanEqualTo(long timeBucket)
  {
    checkpointManager.setLatestExpiredTimeBucket(timeBucket);
  }

  @Override
  public OperatorContext getOperatorContext()
  {
    return operatorContext;
  }

  @Override
  public void setMaxMemorySize(long bytes)
  {
    maxMemorySize = bytes;
  }

  /**
   *
   * @return the optimal size of the cache that triggers eviction of committed data from memory.
   */
  public long getMaxMemorySize()
  {
    return maxMemorySize;
  }

  /**
   * Sets the {@link FileAccess} implementation.
   * @param fileAccess specific implementation of FileAccess.
   */
  public void setFileAccess(@NotNull FileAccess fileAccess)
  {
    this.fileAccess = Preconditions.checkNotNull(fileAccess);
  }

  @Override
  public FileAccess getFileAccess()
  {
    return fileAccess;
  }

  /**
   * Sets the time bucket assigner. This can be used for plugging any custom time bucket assigner.
   *
   * @param timeBucketAssigner a {@link TimeBucketAssigner}
   */
  public void setTimeBucketAssigner(@NotNull TimeBucketAssigner timeBucketAssigner)
  {
    this.timeBucketAssigner = Preconditions.checkNotNull(timeBucketAssigner);
  }

  @Override
  public TimeBucketAssigner getTimeBucketAssigner()
  {
    return timeBucketAssigner;
  }

  @Override
  public Comparator<Slice> getKeyComparator()
  {
    return keyComparator;
  }

  /**
   * Sets the key comparator. The keys on the disk in time bucket files are sorted. This sets the comparator for the
   * key.
   * @param keyComparator key comparator
   */
  public void setKeyComparator(@NotNull Comparator<Slice> keyComparator)
  {
    this.keyComparator = Preconditions.checkNotNull(keyComparator);
  }

  @Override
  public BucketsFileSystem getBucketsFileSystem()
  {
    return bucketsFileSystem;
  }

  /**
   * @return number of worker threads in the reader service.
   */
  public int getNumReaders()
  {
    return numReaders;
  }

  /**
   * Sets the number of worker threads in the reader service which is responsible for asynchronously fetching
   * values of the keys. This should not exceed number of buckets.
   *
   * @param numReaders number of worker threads in the reader service.
   */
  public void setNumReaders(int numReaders)
  {
    this.numReaders = numReaders;
  }

  /**
   * @return regular interval at which the size of state is checked.
   */
  public Duration getCheckStateSizeInterval()
  {
    return checkStateSizeInterval;
  }

  /**
   * Sets the interval at which the size of state is regularly checked.

   * @param checkStateSizeInterval regular interval at which the size of state is checked.
   */
  public void setCheckStateSizeInterval(@NotNull Duration checkStateSizeInterval)
  {
    this.checkStateSizeInterval = Preconditions.checkNotNull(checkStateSizeInterval);
  }

  /**
   * @return duration which prevents a bucket being evicted.
   */
  public Duration getDurationPreventingFreeingSpace()
  {
    return durationPreventingFreeingSpace;
  }

  /**
   * Sets the duration which prevents buckets to free space. For example if this is set to an hour, then only
   * buckets which were not accessed in last one hour will be triggered to free spaces.
   *
   * @param durationPreventingFreeingSpace time duration
   */
  public void setDurationPreventingFreeingSpace(Duration durationPreventingFreeingSpace)
  {
    this.durationPreventingFreeingSpace = durationPreventingFreeingSpace;
  }

  public IncrementalCheckpointManager getCheckpointManager()
  {
    return checkpointManager;
  }

  public void setCheckpointManager(@NotNull IncrementalCheckpointManager checkpointManager)
  {
    this.checkpointManager = Preconditions.checkNotNull(checkpointManager);
  }

  static class ValueFetchTask implements Callable<Slice>
  {
    private final Bucket bucket;
    private final long timeBucketId;
    private final Slice key;
    private final AbstractManagedStateImpl managedState;

    ValueFetchTask(@NotNull Bucket bucket, @NotNull Slice key, long timeBucketId, AbstractManagedStateImpl managedState)
    {
      this.bucket = Preconditions.checkNotNull(bucket);
      this.timeBucketId = timeBucketId;
      this.key = Preconditions.checkNotNull(key);
      this.managedState = Preconditions.checkNotNull(managedState);
    }

    @Override
    public Slice call() throws Exception
    {
      try {
        synchronized (bucket) {
          //a particular bucket should only be handled by one thread at any point of time. Handling of bucket here
          //involves creating readers for the time buckets and de-serializing key/value from a reader.
          Slice value = bucket.get(key, timeBucketId, Bucket.ReadSource.ALL);
          managedState.tasksPerBucketId.remove(bucket.getBucketId(), this);
          return value;
        }
      } catch (Throwable t) {
        managedState.throwable.set(t);
        throw Throwables.propagate(t);
      }
    }
  }

  @VisibleForTesting
  void setStateTracker(@NotNull StateTracker stateTracker)
  {
    this.stateTracker = Preconditions.checkNotNull(stateTracker, "state tracker");
  }

  @VisibleForTesting
  void setBucketsFileSystem(@NotNull BucketsFileSystem bucketsFileSystem)
  {
    this.bucketsFileSystem = Preconditions.checkNotNull(bucketsFileSystem, "buckets file system");
  }

  private static final transient Logger LOG = LoggerFactory.getLogger(AbstractManagedStateImpl.class);
}
