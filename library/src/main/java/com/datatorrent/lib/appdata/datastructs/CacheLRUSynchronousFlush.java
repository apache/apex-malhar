/*
 * Copyright (c) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.appdata.datastructs;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This is an LRU cache.
 * @param <KEY> The type of keys in the cache.
 * @param <VALUE> The type of values in the cache.
 */
public class CacheLRUSynchronousFlush<KEY, VALUE>
{
  public static final int DEFAULT_FLUSHED_SIZE  = 50000;

  private Map<KEY, VALUE> keyToValue = Maps.newHashMap();
  private Long2ObjectSortedMap<Set<KEY>> timeStampToKey = new Long2ObjectAVLTreeMap<Set<KEY>>();
  private Map<KEY, Long> keyToTimeStamp = new HashMap<KEY, Long>();
  private Set<KEY> changed = Sets.newHashSet();
  private int flushedSize = DEFAULT_FLUSHED_SIZE;

  private CacheFlushListener<KEY, VALUE> flushListener;

  public CacheLRUSynchronousFlush(CacheFlushListener<KEY, VALUE> flushListener)
  {
    setFlushListener(flushListener);
  }

  public CacheLRUSynchronousFlush(int flushedSize,
                                  CacheFlushListener<KEY, VALUE> flushListener)
  {
    setFlushedSizePri(flushedSize);
    setFlushListener(flushListener);
  }

  private void setFlushListener(CacheFlushListener<KEY, VALUE> flushListener)
  {
    Preconditions.checkNotNull(flushListener);
    this.flushListener = flushListener;
  }

  /**
   * @return the flushedSize
   */
  public int getFlushedSize()
  {
    return flushedSize;
  }

  /**
   * @param flushedSize the flushedSize to set
   */
  public void setFlushedSize(int flushedSize)
  {
    setFlushedSizePri(flushedSize);
  }

  private void setFlushedSizePri(int flushedSize)
  {
    Preconditions.checkArgument(flushedSize > 0, "The flushedSize must be positive.");
    this.flushedSize = flushedSize;
  }

  public VALUE put(KEY key, VALUE value)
  {
    long timeStamp = System.currentTimeMillis();
    return put(timeStamp, key, value);
  }

  public VALUE put(long timeStamp, KEY key, VALUE value)
  {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);

    changed.add(key);

    Long oldTimeStamp = keyToTimeStamp.put(key, timeStamp);

    if(oldTimeStamp == null || oldTimeStamp != timeStamp) {
      Set<KEY> keys = timeStampToKey.get(timeStamp);

      if(keys == null) {
        keys = Sets.newHashSet();
        timeStampToKey.put(timeStamp, keys);
      }

      keys.add(key);
    }

    if(oldTimeStamp != null) {
      timeStampToKey.get(oldTimeStamp).remove(key);
    }

    return keyToValue.put(key, value);
  }

  public VALUE get(KEY key)
  {
    Preconditions.checkNotNull(key);
    return keyToValue.get(key);
  }

  public VALUE remove(KEY key)
  {
    Preconditions.checkNotNull(key);

    Long timeStamp = keyToTimeStamp.get(key);
    if(timeStamp != null) {
      keyToTimeStamp.remove(key);
      Set<KEY> keys = timeStampToKey.get(timeStamp);
      keys.remove(key);
    }

    changed.remove(key);
    return keyToValue.remove(key);
  }

  public void removeExcess()
  {
    int currentSize = keyToValue.size();

    while(currentSize > flushedSize) {
      long firstKey = timeStampToKey.firstLongKey();
      Set<KEY> keys = timeStampToKey.get(firstKey);

      Iterator<KEY> keyIterator = keys.iterator();

      while(keyIterator.hasNext() && currentSize > flushedSize) {
        KEY key = keyIterator.next();
        VALUE value = keyToValue.remove(key);

        if(value == null) {
          continue;
        }

        currentSize--;
        changed.remove(key);
        keyToTimeStamp.remove(key);
        flushListener.flush(key, value);
      }

      if(keys.isEmpty()) {
        timeStampToKey.remove(firstKey);
      }
    }
  }

  public void flushChanges()
  {
    for(KEY key: changed) {
      VALUE value = keyToValue.get(key);
      flushListener.flush(key, value);
    }

    changed.clear();
  }

  public void removeExcessAndFlushChanges()
  {
    removeExcess();
    flushChanges();
  }

  /**
   * An interface for a listener whose callback is called when the cache is flushed.
   * @param <KEY> The type of keys in the cache.
   * @param <VALUE> The type of values in the cache.
   */
  public interface CacheFlushListener<KEY, VALUE>
  {
    /**
     * The method that is called when a key-value pair is flushed from the cache.
     * @param key The value of the key for the pair that is flushed from the cache.
     * @param value The value of the value for the pair that is flushed from the cache.
     */
    public void flush(KEY key, VALUE value);
  }
}
