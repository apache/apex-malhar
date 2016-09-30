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
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.AbstractFSWindowStateManager;
import org.apache.apex.malhar.lib.wal.FileSystemWAL;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Queues;
import com.google.common.primitives.Longs;

import com.datatorrent.api.Context;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.netlet.util.Slice;

/**
 * Manages state which is written to files by windows. The state from the window files are then transferred to bucket
 * data files. This class listens to time expiry events issued by {@link TimeBucketAssigner}.
 *
 * This component is also responsible for purging old time buckets.
 *
 * @since 3.4.0
 */
public class IncrementalCheckpointManagerImpl extends AbstractFSWindowStateManager
    implements IncrementalCheckpointManager
{
  private static final String WAL_RELATIVE_PATH = "managed_state";

  //windowId => (bucketId => data)
  private final transient Map<Long, Map<Long, Map<Slice, Bucket.BucketedValue>>> savedWindows = new
      ConcurrentSkipListMap<>();

  private transient ExecutorService writerService;
  private transient volatile boolean transfer;

  private final transient LinkedBlockingQueue<Long> windowsToTransfer = Queues.newLinkedBlockingQueue();
  private final transient AtomicReference<Throwable> throwable = new AtomicReference<>();

  protected transient ManagedStateContext managedStateContext;

  private final transient AtomicLong latestExpiredTimeBucket = new AtomicLong(-1);

  private transient int waitMillis;
  private volatile long lastTransferredWindow = Stateless.WINDOW_ID;

  private transient long largestWindowAddedToTransferQueue = Stateless.WINDOW_ID;

  public IncrementalCheckpointManagerImpl()
  {
    super();
    setStatePath(WAL_RELATIVE_PATH);
    setRelyOnCheckpoints(true);
  }

  @Override
  public void setup(@NotNull final ManagedStateContext managedStateContext)
  {
    this.managedStateContext = Preconditions.checkNotNull(managedStateContext, "managed state context");
    waitMillis = managedStateContext.getOperatorContext().getValue(Context.OperatorContext.SPIN_MILLIS);
    super.setup(managedStateContext.getOperatorContext());

    writerService = Executors.newSingleThreadExecutor(new NameableThreadFactory("managed-state-writer"));
    transfer = true;
    writerService.submit(new Runnable()
    {
      @Override
      public void run()
      {
        while (transfer) {
          transferWindowFiles();
          if (latestExpiredTimeBucket.get() > -1) {
            try {
              managedStateContext.getBucketsFileSystem().deleteTimeBucketsLessThanEqualTo(
                  latestExpiredTimeBucket.getAndSet(-1));
            } catch (IOException e) {
              throwable.set(e);
              LOG.debug("delete files", e);
              Throwables.propagate(e);
            }
          }
        }
      }
    });
  }

  /**
   * Retrieves artifacts available for all the windows saved by the enclosing partition.
   * @return  artifact saved per window.
   * @throws IOException
   */
  @Override
  public Map<Long, Map<Long, Map<Slice, Bucket.BucketedValue>>> retrieveAllWindows() throws IOException
  {
    FileSystemWAL.FileSystemWALReader reader = getWal().getReader();
    reader.seek(getWal().getWalStartPointer());

    Slice windowSlice = readNext(reader);
    while (reader.getCurrentPointer().compareTo(getWal().getWalEndPointerAfterRecovery()) < 0 && windowSlice != null) {
      long window = Longs.fromByteArray(windowSlice.toByteArray());
      @SuppressWarnings("unchecked")
      Map<Long, Map<Slice, Bucket.BucketedValue>> artifact =
          (Map<Long, Map<Slice, Bucket.BucketedValue>>)fromSlice(readNext(reader));

      //add the artifact to cache so that it can be transferred to bucket files
      addAdditionalBucketDataForWindow(window, savedWindows, artifact);

      windowSlice = readNext(reader); //null or next window
    }
    reader.seek(getWal().getWalStartPointer());

    closeReadersIfNecessary();
    return ImmutableMap.copyOf(savedWindows);
  }

  protected void transferWindowFiles()
  {
    try {
      Long windowId = windowsToTransfer.poll();
      if (windowId != null) {
        try {
          LOG.debug("transfer window {}", windowId);
          //bucket id => bucket data(key => value, time-buckets)
          Map<Long, Map<Slice, Bucket.BucketedValue>> buckets = savedWindows.remove(windowId);

          for (Map.Entry<Long, Map<Slice, Bucket.BucketedValue>> singleBucket : buckets.entrySet()) {
            managedStateContext.getBucketsFileSystem().writeBucketData(windowId, singleBucket.getKey(),
                singleBucket.getValue());
          }
          committed(windowId);
        } catch (Throwable t) {
          throwable.set(t);
          LOG.debug("transfer window {}", windowId, t);
          Throwables.propagate(t);
        }

        this.lastTransferredWindow = windowId;
      } else {
        Thread.sleep(waitMillis);
      }
    } catch (InterruptedException ex) {
      //sleep can be interrupted by teardown so no need to re-throw interrupt exception
      LOG.debug("interrupted", ex);
    }
  }

  /**
   * The unsaved state combines data received in multiple windows. This window data manager persists this data
   * on disk by the window id in which it was requested.
   * @param windowId  window id.
   * @param buckets   un-saved data of all buckets.
   *
   * @throws IOException
   */
  @Override
  public void writeBuckets(long windowId, Map<Long, Map<Slice, Bucket.BucketedValue>> buckets) throws IOException
  {
    Throwable lthrowable;
    if ((lthrowable = throwable.get()) != null) {
      LOG.error("Error while transferring");
      Throwables.propagate(lthrowable);
    }

    addAdditionalBucketDataForWindow(windowId, savedWindows, buckets);
    FileSystemWAL.FileSystemWALWriter writer = wal.getWriter();

    appendWindowId(writer, windowId);
    writer.append(toSlice(buckets));
    writer.rotateIfNecessary();
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    wal.beforeCheckpoint(windowId);
  }

  /**
   * Transfers the data which has been committed till windowId to data files.
   *
   * @param committedWindowId   window id
   */
  @Override
  public void committed(long committedWindowId)
  {
    LOG.debug("data manager committed {}", committedWindowId);
    for (Long currentWindow : savedWindows.keySet()) {
      if (currentWindow <= largestWindowAddedToTransferQueue) {
        continue;
      }
      if (currentWindow <= committedWindowId) {
        LOG.debug("to transfer {}", currentWindow);
        largestWindowAddedToTransferQueue = currentWindow;
        windowsToTransfer.add(currentWindow);
      } else {
        break;
      }
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    transfer = false;
    writerService.shutdownNow();
  }

  @Override
  public void purgeTimeBucketsLessThanEqualTo(long timeBucket)
  {
    latestExpiredTimeBucket.set(timeBucket);
  }

  /**
   * Gets the last windowId for which data was successfully merged with a bucket data file.
   * @return The last windowId for which data was successfully merged with a bucket data file.
   */
  @Override
  public long getLastTransferredWindow()
  {
    return lastTransferredWindow;
  }

  static void addAdditionalBucketDataForWindow(long window, Map<Long,
      Map<Long, Map<Slice, Bucket.BucketedValue>>> target, Map<Long, Map<Slice, Bucket.BucketedValue>> addition)
  {
    Map<Long, Map<Slice, Bucket.BucketedValue>> existingWindowData = target.get(window);
    if (existingWindowData == null) {
      //there isn't any existing data for window
      existingWindowData = addition;
      target.put(window, existingWindowData);
    } else {
      for (Map.Entry<Long, Map<Slice, Bucket.BucketedValue>> entry : addition.entrySet()) {
        long bucketId = entry.getKey();
        Map<Slice, Bucket.BucketedValue> existingBucketData = existingWindowData.get(bucketId);
        if (existingBucketData == null) {
          //there isn't any existing data for the bucket
          existingWindowData.put(bucketId, entry.getValue());
        } else {
          //add bucket data.
          existingBucketData.putAll(entry.getValue());
        }
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(IncrementalCheckpointManagerImpl.class);

}
