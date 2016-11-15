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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.BucketedState;
import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.utils.serde.AffixKeyValueSerdeManager;
import org.apache.apex.malhar.lib.utils.serde.BufferSlice;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * A Spillable implementation of {@link Map}
 * @param <K> The types of keys.
 * @param <V> The types of values.
 *
 * @since 3.5.0
 */
@DefaultSerializer(FieldSerializer.class)
@InterfaceStability.Evolving
public class SpillableMapImpl2<K, V> implements Spillable.SpillableMap<K, V>, Spillable.SpillableComponent,
    Serializable
{
  private static final long serialVersionUID = 4552547110215784584L;
  private transient WindowBoundedMapCache<K, V> cache = new WindowBoundedMapCache<>();
  private transient Input tmpInput = new Input();

  private TimeExtractor<V> timeExtractorFromValue;

  @NotNull
  private SpillableStateStore store;

  private int size = 0;

  protected AffixKeyValueSerdeManager<K, V> keyValueSerdeManager;

  // LRU cache for key and latest time of values for the key
  private transient Cache<K, Long> keyToLatestTime = CacheBuilder.newBuilder().build();

  private SpillableMapImpl2()
  {
    //for kryo
  }


  /**
   * Creats a {@link SpillableMapImpl}.
   * @param store The {@link SpillableStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableMapImpl}.
   * {@link SpillableMapImpl} in the provided {@link SpillableStateStore}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
   * @param timeExtractorFromValue Extract time from the value and use it to decide where the data goes
   */
  public SpillableMapImpl2(SpillableStateStore store, byte[] identifier, Serde<K> serdeKey,
      Serde<V> serdeValue, TimeExtractor<V> timeExtractorFromValue)
  {
    this.store = Preconditions.checkNotNull(store);
    keyValueSerdeManager = new AffixKeyValueSerdeManager<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
    this.timeExtractorFromValue = timeExtractorFromValue;
  }

  public SpillableStateStore getStore()
  {
    return this.store;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public boolean isEmpty()
  {
    return size == 0;
  }

  @Override
  public boolean containsKey(Object o)
  {
    return get(o) != null;
  }

  @Override
  public boolean containsValue(Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public V get(Object o)
  {
    V result = null;
    K key = (K)o;

    if (cache.getRemovedKeys().contains(key)) {
      return null;
    }

    V val = cache.get(key);

    if (val != null) {
      return val;
    }

    Slice valSlice;
    Long latestTimeForKey = keyToLatestTime.getIfPresent(key);
    if (latestTimeForKey != null) {
      valSlice = store.getSync(latestTimeForKey, keyValueSerdeManager.serializeDataKey(key, false));
    } else {
      // look up all time buckets
      valSlice = store.getSync(-1, keyValueSerdeManager.serializeDataKey(key, false));
    }

    if (valSlice == null || valSlice == BucketedState.EXPIRED || valSlice.length == 0) {
      return null;
    }

    tmpInput.setBuffer(valSlice.buffer, valSlice.offset, valSlice.length);
    result = keyValueSerdeManager.deserializeValue(tmpInput);
    // find slice not in the cache, so update it
    keyToLatestTime.put(key, timeExtractorFromValue.getTime(result));
    return result;
  }


  @Override
  public V put(K k, V v)
  {

    cache.put(k, v);
    V value = get(k);

    if (value == null) {
      size++;
    }

    if (value == null || timeExtractorFromValue.getTime(v) > timeExtractorFromValue.getTime(value)) {
      keyToLatestTime.put(k, timeExtractorFromValue.getTime(v));
    }

    return value;
  }

  @Override
  public V remove(Object o)
  {
    V value = get(o);

    if (value != null) {
      size--;
    }

    keyToLatestTime.invalidate(o);
    cache.remove((K)o);

    return value;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map)
  {
    for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<K> keySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<V> values()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Entry<K, V>> entrySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    store.ensureBucket(0);
    keyValueSerdeManager.setup(store, 0);
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    for (K key: cache.getChangedKeys()) {
      store.put(timeExtractorFromValue.getTime(cache.get(key)), keyValueSerdeManager.serializeDataKey(key, true),
          keyValueSerdeManager.serializeValue(cache.get(key)));
    }

    for (K key: cache.getRemovedKeys()) {

      store.put(timeExtractorFromValue.getTime(cache.get(key)), keyValueSerdeManager.serializeDataKey(key, true), BufferSlice.EMPTY_SLICE);
    }
    cache.endWindow();
    keyValueSerdeManager.resetReadBuffer();
  }

  @Override
  public void teardown()
  {
  }

}
