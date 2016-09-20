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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.utils.serde.PassThruSliceSerde;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerdeIntSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdePairSlice;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
public class SpillableSetMultimapImpl<K, V> implements Spillable.SpillableSetMultimap<K, V>,
    Spillable.SpillableComponent
{
  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final byte[] META_KEY_SUFFIX = new byte[]{(byte)0, (byte)0, (byte)0};

  private transient WindowBoundedMapCache<K, SpillableSetImpl<V>> cache = new WindowBoundedMapCache<>();

  @NotNull
  private SpillableByteMapImpl<Slice, Pair<Integer, V>> map;
  private SpillableStateStore store;
  private byte[] identifier;
  private long bucket;
  private Serde<K, Slice> serdeKey;
  private Serde<V, Slice> serdeValue;
  private transient List<SpillableSetImpl<V>> removedSets = new ArrayList<>();

  private SpillableSetMultimapImpl()
  {
    // for kryo
  }

  /**
   * Creates a {@link SpillableSetMultimapImpl}.
   * @param store The {@link SpillableStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableSetMultimapImpl}.
   * @param bucket The Id of the bucket used to store this
   * {@link SpillableSetMultimapImpl} in the provided {@link SpillableStateStore}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing values.
   */
  public SpillableSetMultimapImpl(SpillableStateStore store, byte[] identifier, long bucket,
      Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    this.store = Preconditions.checkNotNull(store);
    this.identifier = Preconditions.checkNotNull(identifier);
    this.bucket = bucket;
    this.serdeKey = Preconditions.checkNotNull(serdeKey);
    this.serdeValue = Preconditions.checkNotNull(serdeValue);

    map = new SpillableByteMapImpl(store, identifier, bucket, new PassThruSliceSerde(), new SerdePairSlice<>(new SerdeIntSlice(), serdeValue));
  }

  public SpillableStateStore getStore()
  {
    return store;
  }

  @Override
  public Set<V> get(@NotNull K key)
  {
    return getHelper(key);
  }

  private SpillableSetImpl<V> getHelper(@NotNull K key)
  {
    SpillableSetImpl<V> spillableSet = cache.get(key);

    if (spillableSet == null) {
      Slice keySlice = serdeKey.serialize(key);
      Pair<Integer, V> meta = map.get(SliceUtils.concatenate(keySlice, META_KEY_SUFFIX));

      if (meta == null) {
        return null;
      }

      Slice keyPrefix = SliceUtils.concatenate(identifier, keySlice);
      spillableSet = new SpillableSetImpl<>(bucket, keyPrefix.toByteArray(), store, serdeValue);
      spillableSet.setSize(meta.getLeft());
      spillableSet.setHead(meta.getRight());
    }

    cache.put(key, spillableSet);

    return spillableSet;
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
  public Set<Map.Entry<K, V>> entries()
  {
    throw new UnsupportedOperationException();
  }

  /**
   * Note that this always returns null because the set is no longer valid after this call
   *
   * @param key
   * @return null
   */
  @Override
  public Set<V> removeAll(@NotNull Object key)
  {
    SpillableSetImpl<V> spillableSet = getHelper((K)key);
    if (spillableSet != null) {
      cache.remove((K)key);
      Slice keySlice = SliceUtils.concatenate(serdeKey.serialize((K)key), META_KEY_SUFFIX);
      map.remove(keySlice);
      spillableSet.clear();
      removedSets.add(spillableSet);
    }
    return null;
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
  public boolean containsKey(Object key)
  {
    if (cache.contains((K)key)) {
      return true;
    }
    Slice keySlice = SliceUtils.concatenate(serdeKey.serialize((K)key), META_KEY_SUFFIX);
    return map.containsKey(keySlice);
  }

  @Override
  public boolean containsValue(@NotNull Object value)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsEntry(Object key, Object value)
  {
    Set<V> set = get((K)key);
    if (set == null) {
      return false;
    } else {
      return set.contains(value);
    }
  }

  @Override
  public boolean put(K key, V value)
  {
    SpillableSetImpl<V> spillableSet = getHelper(key);

    if (spillableSet == null) {
      Slice keyPrefix = SliceUtils.concatenate(identifier, serdeKey.serialize(key));
      spillableSet = new SpillableSetImpl<>(bucket, keyPrefix.toByteArray(), store, serdeValue);
      cache.put(key, spillableSet);
    }
    spillableSet.add(value);
    return true;
  }

  @Override
  public boolean remove(@NotNull Object key, @NotNull Object value)
  {
    Set<V> set = get((K)key);
    if (set == null) {
      return false;
    } else {
      return set.remove(value);
    }
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
  public Set<V> replaceValues(K key, Iterable<? extends V> values)
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
  }

  @Override
  public void beginWindow(long windowId)
  {
    map.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    for (K key: cache.getChangedKeys()) {

      SpillableSetImpl<V> spillableSet = cache.get(key);
      spillableSet.endWindow();

      map.put(SliceUtils.concatenate(serdeKey.serialize(key), META_KEY_SUFFIX),
          new ImmutablePair<>(spillableSet.size(), spillableSet.getHead()));
    }

    for (SpillableSetImpl removedSet : removedSets) {
      removedSet.endWindow();
    }

    cache.endWindow();
    map.endWindow();
  }

  @Override
  public void teardown()
  {
    map.teardown();
  }
}
