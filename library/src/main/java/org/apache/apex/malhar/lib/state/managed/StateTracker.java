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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.validation.constraints.NotNull;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Tracks the size of state in memory and evicts buckets.
 */
class StateTracker extends TimerTask
{
  //bucket id -> bucket id & time wrapper
  private final transient ConcurrentHashMap<Long, BucketIdTimeWrapper> bucketAccessTimes = new ConcurrentHashMap<>();

  private transient ConcurrentSkipListSet<BucketIdTimeWrapper> bucketHeap;

  private final transient Timer memoryFreeService = new Timer();

  protected transient AbstractManagedStateImpl managedStateImpl;

  void setup(@NotNull AbstractManagedStateImpl managedStateImpl)
  {
    this.managedStateImpl = Preconditions.checkNotNull(managedStateImpl, "managed state impl");

    this.bucketHeap = new ConcurrentSkipListSet<>(
        new Comparator<BucketIdTimeWrapper>()
        {
          //Note: this comparator imposes orderings that are inconsistent with equals.
          @Override
          public int compare(BucketIdTimeWrapper o1, BucketIdTimeWrapper o2)
          {
            if (o1.getLastAccessedTime() < o2.getLastAccessedTime()) {
              return -1;
            }
            if (o1.getLastAccessedTime() > o2.getLastAccessedTime()) {
              return 1;
            }

            return Long.compare(o1.bucketId, o2.bucketId);
          }
        });
    long intervalMillis = managedStateImpl.getCheckStateSizeInterval().getMillis();
    memoryFreeService.scheduleAtFixedRate(this, intervalMillis, intervalMillis);
  }

  void bucketAccessed(long bucketId)
  {
    BucketIdTimeWrapper idTimeWrapper = bucketAccessTimes.get(bucketId);
    if (idTimeWrapper != null) {
      bucketHeap.remove(idTimeWrapper);
    }  else {
      idTimeWrapper = new BucketIdTimeWrapper(bucketId);
    }
    idTimeWrapper.setLastAccessedTime(System.currentTimeMillis());
    bucketHeap.add(idTimeWrapper);
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public void run()
  {
    synchronized (managedStateImpl.commitLock) {
      //freeing of state needs to be stopped during commit as commit results in transferring data to a state which
      // can be freed up as well.
      long bytesSum = 0;
      for (Bucket bucket : managedStateImpl.buckets) {
        if (bucket != null) {
          bytesSum += bucket.getSizeInBytes();
        }
      }

      if (bytesSum > managedStateImpl.getMaxMemorySize()) {
        Duration duration = managedStateImpl.getDurationPreventingFreeingSpace();
        long durationMillis = 0;
        if (duration != null) {
          durationMillis = duration.getMillis();
        }

        BucketIdTimeWrapper idTimeWrapper;
        while (bytesSum > managedStateImpl.getMaxMemorySize() && bucketHeap.size() > 0 &&
            null != (idTimeWrapper = bucketHeap.first())) {
          //trigger buckets to free space

          if (System.currentTimeMillis() - idTimeWrapper.getLastAccessedTime() < durationMillis) {
            //if the least recently used bucket cannot free up space because it was accessed within the
            //specified duration then subsequent buckets cannot free space as well because this heap is ordered by time.
            break;
          }
          long bucketId = idTimeWrapper.bucketId;
          Bucket bucket = managedStateImpl.getBucket(bucketId);
          if (bucket != null) {

            synchronized (bucket) {
              long sizeFreed;
              try {
                sizeFreed = bucket.freeMemory(managedStateImpl.getCheckpointManager().getLastTransferredWindow());
                LOG.debug("bucket freed {} {}", bucketId, sizeFreed);
              } catch (IOException e) {
                managedStateImpl.throwable.set(e);
                throw new RuntimeException("freeing " + bucketId, e);
              }
              bytesSum -= sizeFreed;
            }
            bucketHeap.remove(idTimeWrapper);
            bucketAccessTimes.remove(bucketId);
          }
        }
      }
    }
  }

  void teardown()
  {
    memoryFreeService.cancel();
  }

  /**
   * Wrapper class for bucket id and the last time the bucket was accessed.
   */
  private static class BucketIdTimeWrapper
  {
    private final long bucketId;
    private long lastAccessedTime;

    BucketIdTimeWrapper(long bucketId)
    {
      this.bucketId = bucketId;
    }

    private synchronized long getLastAccessedTime()
    {
      return lastAccessedTime;
    }

    private synchronized void setLastAccessedTime(long lastAccessedTime)
    {
      this.lastAccessedTime = lastAccessedTime;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BucketIdTimeWrapper)) {
        return false;
      }

      BucketIdTimeWrapper that = (BucketIdTimeWrapper)o;
      //Note: the comparator used with bucket heap imposes orderings that are inconsistent with equals
      return bucketId == that.bucketId;

    }

    @Override
    public int hashCode()
    {
      return (int)(bucketId ^ (bucketId >>> 32));
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(StateTracker.class);

}
