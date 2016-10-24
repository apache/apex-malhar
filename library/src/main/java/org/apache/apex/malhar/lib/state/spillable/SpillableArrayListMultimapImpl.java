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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.utils.serde.AffixKeyValueSerdeManager;
import org.apache.apex.malhar.lib.utils.serde.IntSerde;
import org.apache.apex.malhar.lib.utils.serde.PassThruSliceSerde;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * This is an implementation of Guava's ListMultimap which spills data to a {@link SpillableStateStore}.
 *
 * @since 3.5.0
 */
@DefaultSerializer(FieldSerializer.class)
@InterfaceStability.Evolving
public class SpillableArrayListMultimapImpl<K, V> implements Spillable.SpillableListMultimap<K, V>,
    Spillable.SpillableComponent
{
  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final byte[] SIZE_KEY_SUFFIX = new byte[]{(byte)0, (byte)0, (byte)0};

  private transient WindowBoundedMapCache<K, SpillableArrayListImpl<V>> cache = new WindowBoundedMapCache<>();
  private transient boolean isRunning = false;
  private transient boolean isInWindow = false;

  private int batchSize = DEFAULT_BATCH_SIZE;
  @NotNull
  private SpillableMapImpl<Slice, Integer> map;
  private SpillableStateStore store;
  private long bucket;
  private Serde<V> valueSerde;

  protected transient Context.OperatorContext context;
  protected AffixKeyValueSerdeManager<K, V> keyValueSerdeManager;

  private SpillableArrayListMultimapImpl()
  {
    // for kryo
  }

  /**
   * Creates a {@link SpillableArrayListMultimapImpl}.
   * @param store The {@link SpillableStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableArrayListMultimapImpl}.
   * @param bucket The Id of the bucket used to store this
   * {@link SpillableArrayListMultimapImpl} in the provided {@link SpillableStateStore}.
   * @param keySerde The {@link Serde} to use when serializing and deserializing keys.
   * @param valueSerde The {@link Serde} to use when serializing and deserializing values.
   */
  public SpillableArrayListMultimapImpl(SpillableStateStore store, byte[] identifier, long bucket,
      Serde<K> keySerde,
      Serde<V> valueSerde)
  {
    this.store = Preconditions.checkNotNull(store);
    this.bucket = bucket;
    this.valueSerde = Preconditions.checkNotNull(valueSerde);

    keyValueSerdeManager = new AffixKeyValueSerdeManager<K, V>(SIZE_KEY_SUFFIX, identifier, Preconditions.checkNotNull(keySerde), valueSerde);

    map = new SpillableMapImpl(store, identifier, bucket, new PassThruSliceSerde(), new IntSerde());
  }

  public SpillableStateStore getStore()
  {
    return store;
  }

  @Override
  public List<V> get(@Nullable K key)
  {
    return getHelper(key);
  }

  private SpillableArrayListImpl<V> getHelper(@Nullable K key)
  {
    SpillableArrayListImpl<V> spillableArrayList = cache.get(key);

    if (spillableArrayList == null) {
      Integer size = map.get(keyValueSerdeManager.serializeMetaKey(key, false));
      if (size == null) {
        return null;
      }

      spillableArrayList = new SpillableArrayListImpl<V>(bucket, keyValueSerdeManager.serializeDataKey(key, false).toByteArray(), store, valueSerde);
      spillableArrayList.setSize(size);
    }

    cache.put(key, spillableArrayList);

    return spillableArrayList;
  }

  @Override
  public Set<K> keySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Multiset<K> keys()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<V> values()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<Map.Entry<K, V>> entries()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<V> removeAll(@Nullable Object key)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size()
  {
    // TODO: This is actually wrong since in a Multimap, size() should return the number of entries, not the number of distinct keys
    return map.size();
  }

  @Override
  public boolean isEmpty()
  {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(@Nullable Object key)
  {
    return cache.contains((K)key) || map.containsKey(keyValueSerdeManager.serializeMetaKey((K)key, false));
  }

  @Override
  public boolean containsValue(@Nullable Object value)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsEntry(@Nullable Object key, @Nullable Object value)
  {
    SpillableArrayListImpl<V> spillableArrayList = getHelper((K)key);
    if (spillableArrayList == null) {
      return false;
    }
    for (int i = 0; i < spillableArrayList.size(); i++) {
      V v = spillableArrayList.get(i);
      if (v == null) {
        if (value == null) {
          return true;
        }
      } else {
        if (v.equals(value)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean put(@Nullable K key, @Nullable V value)
  {
    SpillableArrayListImpl<V> spillableArrayList = getHelper(key);

    if (spillableArrayList == null) {
      Slice keyPrefix = keyValueSerdeManager.serializeDataKey(key, true);
      spillableArrayList = new SpillableArrayListImpl<V>(bucket, keyPrefix.toByteArray(), store, valueSerde);
      spillableArrayList.setup(context);
      cache.put(key, spillableArrayList);
    }

    spillableArrayList.add(value);
    return true;
  }

  @Override
  public boolean remove(@Nullable Object key, @Nullable Object value)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean putAll(@Nullable K key, Iterable<? extends V> values)
  {
    boolean changed = false;

    for (V value: values) {
      changed |= put(key, value);
    }

    return changed;
  }

  @Override
  public boolean putAll(Multimap<? extends K, ? extends V> multimap)
  {
    boolean changed = false;

    for (Map.Entry<? extends K, ? extends V> entry: multimap.entries()) {
      changed |= put(entry.getKey(), entry.getValue());
    }

    return changed;
  }

  @Override
  public List<V> replaceValues(K key, Iterable<? extends V> values)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<K, Collection<V>> asMap()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.context = context;

    map.setup(context);
    isRunning = true;

    keyValueSerdeManager.setup(store, bucket);
  }

  @Override
  public void beginWindow(long windowId)
  {
    map.beginWindow(windowId);
    keyValueSerdeManager.beginWindow(windowId);
    isInWindow = true;
  }

  @Override
  public void endWindow()
  {
    isInWindow = false;
    for (K key: cache.getChangedKeys()) {

      SpillableArrayListImpl<V> spillableArrayList = cache.get(key);
      spillableArrayList.endWindow();

      map.put(keyValueSerdeManager.serializeMetaKey(key, true), spillableArrayList.size());
    }

    Preconditions.checkState(cache.getRemovedKeys().isEmpty());
    cache.endWindow();
    map.endWindow();

    keyValueSerdeManager.resetReadBuffer();
  }

  @Override
  public void teardown()
  {
    isRunning = false;
    map.teardown();
  }
}
