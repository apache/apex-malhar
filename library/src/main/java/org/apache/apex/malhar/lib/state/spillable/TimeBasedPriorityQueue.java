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
package org.apache.apex.malhar.lib.state.spillable;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A simple priority queue where the priority of an object is determined by the time at which it is inserted into the
 * queue. The object in the queue with the smallest time stamp is the first to be dequeued.
 * @param <T> The type of the objects inserted into the queue.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class TimeBasedPriorityQueue<T>
{
  private Map<T, TimeWrapper<T>> timeWrappperMap = Maps.newHashMap();
  private Set<TimeWrapper<T>> sortedTimestamp = Sets.newTreeSet();

  public void upSert(T value)
  {
    TimeWrapper<T> timeWrapper = timeWrappperMap.get(value);

    if (timeWrapper != null) {
      sortedTimestamp.remove(timeWrapper);
      timeWrapper.setTimestamp(System.currentTimeMillis());
    } else {
      timeWrapper = new TimeWrapper<>(value, System.currentTimeMillis());
      timeWrappperMap.put(value, timeWrapper);
    }

    sortedTimestamp.add(timeWrapper);
  }

  public void remove(T value)
  {
    TimeWrapper<T> timeWrapper = timeWrappperMap.get(value);
    sortedTimestamp.remove(timeWrapper);
    timeWrappperMap.remove(value);
  }

  public Set<T> removeLRU(int count)
  {
    Preconditions.checkArgument(count > 0 && count <= timeWrappperMap.size());

    Iterator<TimeWrapper<T>> iterator = sortedTimestamp.iterator();
    Set<T> valueSet = Sets.newHashSet();

    for (int counter = 0; counter < count; counter++) {
      T value = iterator.next().getKey();
      valueSet.add(value);
      timeWrappperMap.remove(value);
      iterator.remove();
    }

    return valueSet;
  }

  protected static class TimeWrapper<T> implements Comparable<TimeWrapper<T>>
  {
    private T key;
    private long timestamp;

    public TimeWrapper(T key, long timestamp)
    {
      this.key = Preconditions.checkNotNull(key);
      this.timestamp = timestamp;
    }

    public T getKey()
    {
      return key;
    }

    public long getTimestamp()
    {
      return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
      this.timestamp = timestamp;
    }

    @Override
    public int compareTo(TimeWrapper<T> timeWrapper)
    {
      if (this.timestamp < timeWrapper.getTimestamp()) {
        return -1;
      } else if (this.timestamp > timeWrapper.getTimestamp()) {
        return 1;
      }

      /**
       * NOTE: the following use the equals() to implement the compareTo() for key.
       * it should be OK as the compareTo() only used by TimeBasedPriorityQueue.sortedTimestamp,
       * which only care about the order of time ( the order for key doesn't matter ).
       * But would cause problem if add other function which depended on the order of the key.
       *
       * Add compare by hashCode when not equals in order to compatible with the interface for most cases.
       * Anyway, the order of key is not guaranteed. And we should not return 0 if not equals
       */
      return key.equals(timeWrapper.key) ? 0 : (hashCode() - timeWrapper.hashCode() <= 0 ? -1 : 1);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TimeWrapper<?> that = (TimeWrapper<?>)o;

      return key.equals(that.key);
    }

    @Override
    public int hashCode()
    {
      return key.hashCode();
    }

    @Override
    public String toString()
    {
      return "TimeWrapper{" +
          "key=" + key +
          ", timestamp=" + timestamp +
          '}';
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TimeBasedPriorityQueue.class);
}
