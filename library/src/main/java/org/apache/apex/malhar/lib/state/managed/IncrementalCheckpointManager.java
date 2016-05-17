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

import org.apache.apex.malhar.lib.wal.WindowDataManager;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Queues;

import com.datatorrent.api.Context;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.netlet.util.Slice;

/**
 * Manages state which is written to files by windows. The state from the window files are then transferred to bucket
 * data files. This class listens to time expiry events issued by {@link TimeBucketAssigner}.
 *
 * This component is also responsible for purging old time buckets.
 */
public class IncrementalCheckpointManager extends WindowDataManager.FSWindowDataManager
    implements ManagedStateComponent
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

  public IncrementalCheckpointManager()
  {
    super();
    setRecoveryPath(WAL_RELATIVE_PATH);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    throw new UnsupportedOperationException("not supported");
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
          storageAgent.delete(managedStateContext.getOperatorContext().getId(), windowId);
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

  @Override
  public void save(Object object, int operatorId, long windowId) throws IOException
  {
    throw new UnsupportedOperationException("doesn't support saving any object");
  }

  /**
   * The unsaved state combines data received in multiple windows. This window data manager persists this data
   * on disk by the window id in which it was requested.
   * @param unsavedData   un-saved data of all buckets.
   * @param operatorId    operator id.
   * @param windowId      window id.
   * @param skipWriteToWindowFile flag that enables/disables saving the window file.
   *
   * @throws IOException
   */
  public void save(Map<Long, Map<Slice, Bucket.BucketedValue>> unsavedData, int operatorId, long windowId,
      boolean skipWriteToWindowFile)
      throws IOException
  {
    Throwable lthrowable;
    if ((lthrowable = throwable.get()) != null) {
      LOG.error("Error while transferring");
      Throwables.propagate(lthrowable);
    }
    savedWindows.put(windowId, unsavedData);

    if (!skipWriteToWindowFile) {
      super.save(unsavedData, operatorId, windowId);
    }
  }

  /**
   * Transfers the data which has been committed till windowId to data files.
   *
   * @param operatorId operator id
   * @param windowId   window id
   */
  @SuppressWarnings("UnusedParameters")
  protected void committed(int operatorId, long windowId) throws IOException, InterruptedException
  {
    LOG.debug("data manager committed {}", windowId);
    for (Long currentWindow : savedWindows.keySet()) {
      if (currentWindow <= largestWindowAddedToTransferQueue) {
        continue;
      }
      if (currentWindow <= windowId) {
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

  public void setLatestExpiredTimeBucket(long timeBucket)
  {
    latestExpiredTimeBucket.set(timeBucket);
  }

  /**
   * Gets the last windowId for which data was successfully merged with a bucket data file.
   * @return The last windowId for which data was successfully merged with a bucket data file.
   */
  public long getLastTransferredWindow()
  {
    return lastTransferredWindow;
  }

  private static final Logger LOG = LoggerFactory.getLogger(IncrementalCheckpointManager.class);

}
