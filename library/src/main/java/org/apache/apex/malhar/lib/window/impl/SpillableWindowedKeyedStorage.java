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
package org.apache.apex.malhar.lib.window.impl;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.ManagedTimeUnifiedStateImpl;
import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponent;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.GenericSerde;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datatorrent.api.Context;

/**
 * Implementation of WindowedKeyedStorage using {@link Spillable} data structures
 *
 * @param <K> The key type
 * @param <V> The value type
 *
 * @since 3.6.0
 */
public class SpillableWindowedKeyedStorage<K, V> implements WindowedStorage.WindowedKeyedStorage<K, V>
{
  @NotNull
  protected SpillableComplexComponent scc;
  protected long bucket;
  protected Serde<Window> windowSerde;
  protected Serde<Pair<Window, K>> windowKeyPairSerde;
  protected Serde<K> keySerde;
  protected Serde<V> valueSerde;

  protected Spillable.SpillableMap<Pair<Window, K>, V> windowKeyToValueMap;
  protected Spillable.SpillableSetMultimap<Window, K> windowToKeysMap;

  private static final Logger LOG = LoggerFactory.getLogger(SpillableWindowedKeyedStorage.class);


  private class KVIterator implements Iterator<Map.Entry<K, V>>
  {
    final Window window;
    final Set<K> keys;
    Iterator<K> iterator;

    KVIterator(Window window)
    {
      this.window = window;
      this.keys = windowToKeysMap.get(window);
      if (this.keys != null) {
        this.iterator = this.keys.iterator();
      }
    }

    @Override
    public boolean hasNext()
    {
      return iterator != null && iterator.hasNext();
    }

    @Override
    public Map.Entry<K, V> next()
    {
      K key = iterator.next();
      return new AbstractMap.SimpleEntry<>(key, windowKeyToValueMap.get(new ImmutablePair<>(window, key)));
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  public SpillableWindowedKeyedStorage()
  {
  }

  public SpillableWindowedKeyedStorage(long bucket,
      Serde<Window> windowSerde, Serde<Pair<Window, K>> windowKeyPairSerde, Serde<K> keySerde, Serde<V> valueSerde)
  {
    this.bucket = bucket;
    this.windowSerde = windowSerde;
    this.windowKeyPairSerde = windowKeyPairSerde;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
  }

  public void setSpillableComplexComponent(SpillableComplexComponent scc)
  {
    this.scc = scc;
  }

  public SpillableComplexComponent getSpillableComplexComponent()
  {
    return this.scc;
  }

  public void setBucket(long bucket)
  {
    this.bucket = bucket;
  }

  public void setWindowSerde(Serde<Window> windowSerde)
  {
    this.windowSerde = windowSerde;
  }

  public void setWindowKeyPairSerde(Serde<Pair<Window, K>> windowKeyPairSerde)
  {
    this.windowKeyPairSerde = windowKeyPairSerde;
  }

  public void setValueSerde(Serde<V> valueSerde)
  {
    this.valueSerde = valueSerde;
  }

  @Override
  public boolean containsWindow(Window window)
  {
    return windowToKeysMap.containsKey(window);
  }

  @Override
  public long size()
  {
    return windowToKeysMap.size();
  }

  @Override
  public void remove(Window window)
  {
    Set<K> keys = windowToKeysMap.get(window);
    if (keys != null) {
      for (K key : keys) {
        windowKeyToValueMap.remove(new ImmutablePair<>(window, key));
      }
    }
    windowToKeysMap.removeAll(window);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (bucket == 0) {
      // choose a bucket that is guaranteed to be unique in Apex
      bucket = (context.getValue(Context.DAGContext.APPLICATION_NAME) + "#" + context.getId()).hashCode();
    }
    // set default serdes
    if (windowSerde == null) {
      windowSerde = new GenericSerde<>();
    }
    if (windowKeyPairSerde == null) {
      windowKeyPairSerde = new GenericSerde<>();
    }
    if (keySerde == null) {
      keySerde = new GenericSerde<>();
    }
    if (valueSerde == null) {
      valueSerde = new GenericSerde<>();
    }

    if (windowKeyToValueMap == null) {
      windowKeyToValueMap = scc.newSpillableMap(windowKeyPairSerde, valueSerde, new WindowKeyPairTimeExtractor());
    }
    if (windowToKeysMap == null) {
      windowToKeysMap = scc.newSpillableSetMultimap(bucket, windowSerde, keySerde, new WindowTimeExtractor());
    }
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void put(Window window, K key, V value)
  {
    if (!windowToKeysMap.containsEntry(window, key)) {
      windowToKeysMap.put(window, key);
    }
    windowKeyToValueMap.put(new ImmutablePair<>(window, key), value);
  }

  @Override
  public Iterable<Map.Entry<K, V>> entries(final Window window)
  {
    return new Iterable<Map.Entry<K, V>>()
    {
      @Override
      public Iterator<Map.Entry<K, V>> iterator()
      {
        return new KVIterator(window);
      }
    };
  }

  @Override
  public V get(Window window, K key)
  {
    return windowKeyToValueMap.get(new ImmutablePair<>(window, key));
  }

  @Override
  public void purge(long horizonMillis)
  {
    SpillableStateStore store = scc.getStore();
    if (store instanceof ManagedTimeUnifiedStateImpl) {
      ManagedTimeUnifiedStateImpl timeState = (ManagedTimeUnifiedStateImpl)store;
      long purgeTimeBucket = horizonMillis - timeState.getTimeBucketAssigner().getBucketSpan().getMillis();
      LOG.debug("Purging state less than equal to {}", purgeTimeBucket);
      timeState.purgeTimeBucketsLessThanEqualTo(purgeTimeBucket);
    }
  }

}
