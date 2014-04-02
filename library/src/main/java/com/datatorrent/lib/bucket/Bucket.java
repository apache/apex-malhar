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

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * <p>
 * The bucket data-structure contains all the events which belong to the same bucket.
 * </p>
 * <p>
 * Events in a bucket are divided in 2 sections: <br/>
 * <ul>
 * <li> Written:
 * Known events which are loaded from a persistent store {@link BucketStore}
 * </li>
 * <li> Un-written: New events which are not persisted yet.</li>
 * </ul>
 * A bucket differentiates between these events by keeping them in 2 separate collections.
 * </p>
 * <p>
 * Buckets can be modified only by {@link BucketManager}.
 * Operators that work with buckets can only query it but not modify it.
 * </p>
 *
 * @param <T> type of bucket events
 */
public class Bucket<T extends Bucketable>
{
  private Map<Object, T> unwrittenEvents;
  public final long bucketKey;

  private transient Map<Object, T> writtenEvents;
  private transient long lastUpdateTime;
  private transient boolean isDataOnDiskLoaded;

  private Bucket()
  {
    bucketKey = -1;
  }

  Bucket(long bucketKey)
  {
    this.bucketKey = Preconditions.checkNotNull(bucketKey, "bucket key");
    this.isDataOnDiskLoaded = false;
    this.lastUpdateTime = System.currentTimeMillis();
  }

  void setWrittenEvents(@Nonnull Map<Object, T> writtenEvents)
  {
    this.writtenEvents = Preconditions.checkNotNull(writtenEvents, "written data");
    isDataOnDiskLoaded = true;
  }

  void setUnwrittenEvents(@Nonnull Map<Object, T> unwrittenEvents)
  {
    this.unwrittenEvents = Preconditions.checkNotNull(unwrittenEvents, "unwritten data");
  }

  void transferDataFromMemoryToStore()
  {
    if (writtenEvents == null) {
      writtenEvents = Maps.newHashMap();
    }
    writtenEvents.putAll(unwrittenEvents);
    unwrittenEvents = null;
  }

  void updateAccessTime()
  {
    lastUpdateTime = System.currentTimeMillis();
  }

  void addNewEvent(Object eventKey, T event)
  {
    if (unwrittenEvents == null) {
      unwrittenEvents = Maps.newHashMap();
    }
    unwrittenEvents.put(eventKey, event);
  }

  Map<Object, T> getWrittenEvents()
  {
    return writtenEvents;
  }

  Map<Object, T> getUnwrittenEvents()
  {
    return unwrittenEvents;
  }

  long lastUpdateTime()
  {
    return lastUpdateTime;
  }

  /**
   * Given an event key, fetches the event from written section of the bucket.
   *
   * @param key event key
   * @return event corresponding to the event key if it is present in the written portion; null otherwise.
   */
  @Nullable
  public T getValueFromWrittenPart(Object key)
  {
    if (writtenEvents == null) {
      return null;
    }
    return writtenEvents.get(key);
  }

  /**
   * Given an event key, fetches the event from un-written section of bucket.
   *
   * @param key event key
   * @return event corresponding to the event key if it is present in the unwritten portion; null otherwise.
   */
  @Nullable
  public T getValueFromUnwrittenPart(Object key)
  {
    if (unwrittenEvents == null) {
      return null;
    }
    return unwrittenEvents.get(key);
  }

  /**
   * Calculates the number of persisted events.
   *
   * @return number of events which are have been written to a persistent store.
   */
  public int countOfWrittenEvents()
  {
    if (writtenEvents == null) {
      return 0;
    }
    return writtenEvents.size();
  }

  /**
   * Calculates the number of events which are not persisted yet.
   *
   * @return number of events which have not been written to a persistent store.
   */
  public int countOfUnwrittenEvents()
  {
    if (unwrittenEvents == null) {
      return 0;
    }
    return unwrittenEvents.size();
  }

  /**
   * Returns whether the bucket data persisted on the disk is loaded.
   *
   * @return true if persisted data is loaded; false otherwise.
   */
  public boolean isDataOnDiskLoaded()
  {
    return isDataOnDiskLoaded;
  }

  /**
   * Finds whether the bucket contains the event.
   *
   * @param event the {@link Bucketable} to search for in the bucket.
   * @return true if bucket has the event; false otherwise.
   */
  public boolean containsEvent(T event)
  {
    if (unwrittenEvents != null && unwrittenEvents.containsKey(event.getEventKey())) {
      return true;
    }
    return writtenEvents != null && writtenEvents.containsKey(event.getEventKey());
  }

  @Override
  public String toString()
  {
    return "Bucket {" + bucketKey + "}";
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Bucket)) {
      return false;
    }

    Bucket bucket = (Bucket) o;

    return bucketKey == bucket.bucketKey;

  }

  @Override
  public int hashCode()
  {
    return (int) (bucketKey ^ (bucketKey >>> 32));
  }
}
