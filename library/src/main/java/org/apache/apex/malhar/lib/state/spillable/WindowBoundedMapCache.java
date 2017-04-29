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

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This is an LRU cache with a maximum size. When the cache size is exceeded, the excess elements are kept in the
 * cache until the end of the window. When the end of the window is reached, the least recently used entries are
 * evicted from the cache.
 * @param <K> The type of the keys.
 * @param <V> The type of the values.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class WindowBoundedMapCache<K, V>
{
  private static final transient Logger logger = LoggerFactory.getLogger(WindowBoundedMapCache.class);
  public static final int DEFAULT_MAX_SIZE = 50000;

  private int maxSize = DEFAULT_MAX_SIZE;

  private Map<K, V> cache = Maps.newHashMap();

  private Set<K> changedKeys = Sets.newHashSet();
  private Set<K> removedKeys = Sets.newHashSet();
  private TimeBasedPriorityQueue<K> priorityQueue = new TimeBasedPriorityQueue<>();

  public WindowBoundedMapCache()
  {
  }

  public WindowBoundedMapCache(int maxSize)
  {
    Preconditions.checkArgument(maxSize > 0);

    this.maxSize = maxSize;
  }

  public void put(K key, V value)
  {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);

    removedKeys.remove(key);
    changedKeys.add(key);
    priorityQueue.upSert(key);

    cache.put(key, value);
  }

  public V get(K key)
  {
    Preconditions.checkNotNull(key);

    return cache.get(key);
  }

  public boolean contains(K key)
  {
    return cache.containsKey(key);
  }

  public void remove(K key)
  {
    Preconditions.checkNotNull(key);
    removedKeys.add(key);
    if (cache.containsKey(key)) {
      cache.remove(key);
      changedKeys.remove(key);
      priorityQueue.remove(key);
    }
  }

  public Set<K> getChangedKeys()
  {
    return changedKeys;
  }

  public Set<K> getRemovedKeys()
  {
    return removedKeys;
  }

  /*
    Note: beginWindow is intentionally not implemented because many users need a cache that does not require
    beginWindow to be called.
   */
  public void endWindow()
  {
    int count = cache.size() - maxSize;

    if (count > 0) {
      Set<K> expiredKeys = priorityQueue.removeLRU(count);

      for (K expiredKey: expiredKeys) {
        cache.remove(expiredKey);
      }
    }

    changedKeys = Sets.newHashSet();
    removedKeys = Sets.newHashSet();
  }
}
