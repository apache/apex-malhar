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

import org.apache.apex.malhar.lib.utils.serde.PassThruByteArraySliceSerde;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerdeIntSlice;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;
import org.apache.commons.lang3.ArrayUtils;
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
public class SpillableByteArrayListMultimapImpl<K, V> implements Spillable.SpillableByteArrayListMultimap<K, V>,
    Spillable.SpillableComponent
{
  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final byte[] SIZE_KEY_SUFFIX = new byte[]{(byte)0, (byte)0, (byte)0};

  private transient WindowBoundedMapCache<K, SpillableArrayListImpl<V>> cache = new WindowBoundedMapCache<>();
  private transient boolean isRunning = false;
  private transient boolean isInWindow = false;

  private int batchSize = DEFAULT_BATCH_SIZE;
  @NotNull
  private SpillableByteMapImpl<byte[], Integer> map;
  private SpillableStateStore store;
  private byte[] identifier;
  private long bucket;
  private Serde<K, Slice> serdeKey;
  private Serde<V, Slice> serdeValue;

  private SpillableByteArrayListMultimapImpl()
  {
    // for kryo
  }

  /**
   * Creates a {@link SpillableByteArrayListMultimapImpl}.
   * @param store The {@link SpillableStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableByteArrayListMultimapImpl}.
   * @param bucket The Id of the bucket used to store this
   * {@link SpillableByteArrayListMultimapImpl} in the provided {@link SpillableStateStore}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing values.
   */
  public SpillableByteArrayListMultimapImpl(SpillableStateStore store, byte[] identifier, long bucket,
      Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    this.store = Preconditions.checkNotNull(store);
    this.identifier = Preconditions.checkNotNull(identifier);
    this.bucket = bucket;
    this.serdeKey = Preconditions.checkNotNull(serdeKey);
    this.serdeValue = Preconditions.checkNotNull(serdeValue);

    map = new SpillableByteMapImpl(store, identifier, bucket, new PassThruByteArraySliceSerde(), new SerdeIntSlice());
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
      Slice keySlice = serdeKey.serialize(key);
      Integer size = map.get(SliceUtils.concatenate(keySlice, SIZE_KEY_SUFFIX).toByteArray());

      if (size == null) {
        return null;
      }

      Slice keyPrefix = SliceUtils.concatenate(identifier, keySlice);
      spillableArrayList = new SpillableArrayListImpl<V>(bucket, keyPrefix.toByteArray(), store, serdeValue);
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
    SpillableArrayListImpl<V> spillableArrayList = getHelper((K)key);

    if (spillableArrayList != null) {
      Slice keySlice = serdeKey.serialize((K)key);
      map.remove(SliceUtils.concatenate(keySlice, SIZE_KEY_SUFFIX).toByteArray());
      cache.remove((K)key);
    }
    // TODO: need to mark this list to be deallocated from managed state
    return spillableArrayList;
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size()
  {
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
    return cache.contains((K)key) || map.containsKey(SliceUtils.concatenate(serdeKey.serialize((K)key),
        SIZE_KEY_SUFFIX).toByteArray());
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
      Slice keyPrefix = SliceUtils.concatenate(identifier, serdeKey.serialize(key));
      spillableArrayList = new SpillableArrayListImpl<V>(bucket, keyPrefix.toByteArray(), store, serdeValue);

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

    for (V value : values) {
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
    map.setup(context);
    isRunning = true;
  }

  @Override
  public void beginWindow(long windowId)
  {
    map.beginWindow(windowId);
    isInWindow = true;
  }

  @Override
  public void endWindow()
  {
    isInWindow = false;
    for (K key : cache.getChangedKeys()) {

      SpillableArrayListImpl<V> spillableArrayList = cache.get(key);
      spillableArrayList.endWindow();

      Integer size = map.put(SliceUtils.concatenate(serdeKey.serialize(key), SIZE_KEY_SUFFIX).toByteArray(),
          spillableArrayList.size());
    }
    for (K key : cache.getRemovedKeys()) {
      // TODO: Need to find out from Tim on exactly what to do here
      map.remove(SliceUtils.concatenate(serdeKey.serialize(key), SIZE_KEY_SUFFIX).toByteArray());
    }

    cache.endWindow();
    map.endWindow();
  }

  @Override
  public void teardown()
  {
    isRunning = false;
    map.teardown();
  }
}
