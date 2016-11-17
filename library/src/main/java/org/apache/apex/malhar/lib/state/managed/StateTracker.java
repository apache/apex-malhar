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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.validation.constraints.NotNull;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Tracks the size of state in memory and evicts buckets.
 */
class StateTracker extends TimerTask
{
  private final transient Timer memoryFreeService = new Timer();

  protected transient AbstractManagedStateImpl managedStateImpl;

  private transient long lastUpdateAccessTime = 0;
  private final transient Set<Long> accessedBucketIds = Sets.newHashSet();
  private final transient LinkedHashMap<Long, MutableLong> bucketLastAccess = new LinkedHashMap<>(16, 0.75f, true);

  private int updateAccessTimeInterval = 500;

  void setup(@NotNull AbstractManagedStateImpl managedStateImpl)
  {
    this.managedStateImpl = Preconditions.checkNotNull(managedStateImpl, "managed state impl");

    long intervalMillis = managedStateImpl.getCheckStateSizeInterval().getMillis();
    memoryFreeService.scheduleAtFixedRate(this, intervalMillis, intervalMillis);
  }

  void bucketAccessed(long bucketId)
  {
    long now = System.currentTimeMillis();
    if (accessedBucketIds.add(bucketId) || now - lastUpdateAccessTime > updateAccessTimeInterval) {
      synchronized (bucketLastAccess) {
        for (long id : accessedBucketIds) {
          MutableLong lastAccessTime = bucketLastAccess.get(id);
          if (lastAccessTime != null) {
            lastAccessTime.setValue(now);
          } else {
            bucketLastAccess.put(id, new MutableLong(now));
          }
        }
      }
      accessedBucketIds.clear();
      lastUpdateAccessTime = now;
    }
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public void run()
  {
    synchronized (managedStateImpl.commitLock) {
      //freeing of state needs to be stopped during commit as commit results in transferring data to a state which
      // can be freed up as well.
      long bytesSum = 0;
      for (Bucket bucket : managedStateImpl.buckets.values()) {
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

        synchronized (bucketLastAccess) {
          long now = System.currentTimeMillis();
          for (Iterator<Map.Entry<Long, MutableLong>> iterator = bucketLastAccess.entrySet().iterator();
              bytesSum > managedStateImpl.getMaxMemorySize() && iterator.hasNext(); ) {
            Map.Entry<Long, MutableLong> entry = iterator.next();
            if (now - entry.getValue().longValue() < durationMillis) {
              break;
            }
            long bucketId = entry.getKey();
            Bucket bucket = managedStateImpl.getBucket(bucketId);
            if (bucket != null) {
              synchronized (bucket) {
                long sizeFreed;
                try {
                  sizeFreed = bucket.freeMemory(managedStateImpl.getCheckpointManager().getLastTransferredWindow());
                } catch (IOException e) {
                  managedStateImpl.throwable.set(e);
                  throw new RuntimeException("freeing " + bucketId, e);
                }
                bytesSum -= sizeFreed;
              }
              if (bucket.getSizeInBytes() == 0) {
                iterator.remove();
              }
            }
          }
        }
      }
    }
  }

  public int getUpdateAccessTimeInterval()
  {
    return updateAccessTimeInterval;
  }

  public void setUpdateAccessTimeInterval(int updateAccessTimeInterval)
  {
    this.updateAccessTimeInterval = updateAccessTimeInterval;
  }

  void teardown()
  {
    memoryFreeService.cancel();
  }

  private static final Logger LOG = LoggerFactory.getLogger(StateTracker.class);

}
