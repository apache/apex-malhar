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
import java.util.concurrent.Future;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.managed.KeyBucketExtractor;
import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
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
 * Implementation of Spillable array list multi-map over SpillableTimeStateStore
 * @param <K> Key type
 * @param <V> Value type
 */
@InterfaceStability.Evolving
@DefaultSerializer(FieldSerializer.class)
public class SpillableTimeSlicedArrayListMultiMapImpl<K,V> implements Spillable.SpillableListMultimap<K, V>,
    Spillable.SpillableComponent
{
  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final byte[] SIZE_KEY_SUFFIX = new byte[]{(byte)0, (byte)0, (byte)0};

  private transient WindowBoundedMapCache<K, SpillableTimeSlicedArrayListImpl<V>> cache = new WindowBoundedMapCache<>();
  private transient boolean isRunning = false;
  private transient boolean isInWindow = false;

  private int batchSize = DEFAULT_BATCH_SIZE;
  @NotNull
  private SpillableTimeSlicedMapImpl<Slice, Integer> map;
  private SpillableTimeStateStore store;
  private long bucket;
  private Serde<V> valueSerde;
  private TimeExtractor timeExtractor;
  private KeyBucketExtractor keyBucketExtractor;

  protected transient Context.OperatorContext context;
  protected AffixKeyValueSerdeManager<K, V> keyValueSerdeManager;

  private SpillableTimeSlicedArrayListMultiMapImpl()
  {
    // for kryo
  }

  /**
   * Creates a {@link SpillableTimeSlicedArrayListMultiMapImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableTimeSlicedArrayListMultiMapImpl}.
   * @param bucket The Id of the bucket used to store this
   * {@link SpillableTimeSlicedArrayListMultiMapImpl} in the provided {@link SpillableTimeStateStore}.
   * @param keySerde The {@link Serde} to use when serializing and deserializing keys.
   * @param valueSerde The {@link Serde} to use when serializing and deserializing values.
   */
  public SpillableTimeSlicedArrayListMultiMapImpl(SpillableTimeStateStore store, byte[] identifier, long bucket,
      Serde<K> keySerde, Serde<V> valueSerde)
  {
    this.store = Preconditions.checkNotNull(store);
    this.bucket = bucket;
    this.valueSerde = Preconditions.checkNotNull(valueSerde);

    keyValueSerdeManager = new AffixKeyValueSerdeManager<K, V>(SIZE_KEY_SUFFIX, identifier, Preconditions.checkNotNull(keySerde), valueSerde);

    map = new SpillableTimeSlicedMapImpl<>(store, identifier, bucket, new PassThruSliceSerde(), new IntSerde());
  }

  /**
   * Creates a {@link SpillableTimeSlicedArrayListMultiMapImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableTimeSlicedArrayListMultiMapImpl}.
   * @param keySerde The {@link Serde} to use when serializing and deserializing keys.
   * @param valueSerde The {@link Serde} to use when serializing and deserializing values.
   * @param timeExtractor Extract time from the each element and use it to decide where the data goes
   * @param keyBucketExtractor Extract bucket id from the each element and use it to decide where the data goes
   */
  public SpillableTimeSlicedArrayListMultiMapImpl(SpillableTimeStateStore store, byte[] identifier,
      Serde<K> keySerde, Serde<V> valueSerde, @NotNull TimeExtractor timeExtractor, @NotNull KeyBucketExtractor keyBucketExtractor)
  {
    this.store = Preconditions.checkNotNull(store);
    this.valueSerde = Preconditions.checkNotNull(valueSerde);
    this.timeExtractor = timeExtractor;
    this.keyBucketExtractor = keyBucketExtractor;

    keyValueSerdeManager = new AffixKeyValueSerdeManager<K, V>(SIZE_KEY_SUFFIX, identifier, Preconditions.checkNotNull(keySerde), valueSerde);

    map = new SpillableTimeSlicedMapImpl<>(store, identifier, new PassThruSliceSerde(), new IntSerde());
  }

  /**
   * Creates a {@link SpillableTimeSlicedArrayListMultiMapImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableTimeSlicedArrayListMultiMapImpl}.
   * @param keySerde The {@link Serde} to use when serializing and deserializing keys.
   * @param valueSerde The {@link Serde} to use when serializing and deserializing values.
   * @param keyBucketExtractor Extract bucket id from the each element and use it to decide where the data goes
   */
  public SpillableTimeSlicedArrayListMultiMapImpl(SpillableTimeStateStore store, byte[] identifier,
      Serde<K> keySerde, Serde<V> valueSerde, @NotNull KeyBucketExtractor keyBucketExtractor)
  {
    this.store = Preconditions.checkNotNull(store);
    this.valueSerde = Preconditions.checkNotNull(valueSerde);
    this.keyBucketExtractor = keyBucketExtractor;

    keyValueSerdeManager = new AffixKeyValueSerdeManager<K, V>(SIZE_KEY_SUFFIX, identifier, Preconditions.checkNotNull(keySerde), valueSerde);

    map = new SpillableTimeSlicedMapImpl<>(store, identifier, new PassThruSliceSerde(), new IntSerde());
  }

  /**
   * Creates a {@link SpillableTimeSlicedArrayListMultiMapImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableTimeSlicedArrayListMultiMapImpl}.
   * @param keySerde The {@link Serde} to use when serializing and deserializing keys.
   * @param valueSerde The {@link Serde} to use when serializing and deserializing values.
   */
  public SpillableTimeSlicedArrayListMultiMapImpl(SpillableTimeStateStore store, byte[] identifier,
      Serde<K> keySerde, Serde<V> valueSerde)
  {
    this.store = Preconditions.checkNotNull(store);
    this.valueSerde = Preconditions.checkNotNull(valueSerde);
    keyValueSerdeManager = new AffixKeyValueSerdeManager<K, V>(SIZE_KEY_SUFFIX, identifier, Preconditions.checkNotNull(keySerde), valueSerde);

    map = new SpillableTimeSlicedMapImpl<>(store, identifier, new PassThruSliceSerde(), new IntSerde());
  }

  public SpillableTimeStateStore getStore()
  {
    return store;
  }

  @Override
  public List<V> get(@Nullable K key)
  {
    return getHelper(key);
  }

  private SpillableTimeSlicedArrayListImpl<V> getHelper(@Nullable K key)
  {
    SpillableTimeSlicedArrayListImpl<V> spillableArrayList = cache.get(key);

    if (spillableArrayList == null) {
      Integer size = map.get(keyValueSerdeManager.serializeMetaKey(key, false));
      if (size == null) {
        return null;
      }

      spillableArrayList = new SpillableTimeSlicedArrayListImpl<V>(getKeyBucket(key), keyValueSerdeManager.serializeDataKey(key, false).toByteArray(), store, valueSerde, timeExtractor);
      spillableArrayList.setSize(size);
    }

    cache.put(key, spillableArrayList);

    return spillableArrayList;
  }

  public Future<List<V>> getAsync(@Nullable K key)
  {
    SpillableTimeSlicedArrayListImpl<V> spillableArrayList = cache.get(key);

    if (spillableArrayList == null) {
      Integer size = map.get(keyValueSerdeManager.serializeMetaKey(key, false));
      if (size == null) {
        return null;
      }

      spillableArrayList = new SpillableTimeSlicedArrayListImpl<V>(getKeyBucket(key), keyValueSerdeManager.serializeDataKey(key, false).toByteArray(), store, valueSerde, timeExtractor);
      spillableArrayList.setSize(size);
    }

    cache.put(key, spillableArrayList);

    return spillableArrayList.getAsync();
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
    SpillableTimeSlicedArrayListImpl<V> spillableArrayList = getHelper((K)key);
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
    SpillableTimeSlicedArrayListImpl<V> spillableArrayList = getHelper(key);

    if (spillableArrayList == null) {
      Slice keyPrefix = keyValueSerdeManager.serializeDataKey(key, true);
      spillableArrayList = new SpillableTimeSlicedArrayListImpl<V>(getKeyBucket(key), keyPrefix.toByteArray(), store, valueSerde, timeExtractor);
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

      SpillableTimeSlicedArrayListImpl<V> spillableArrayList = cache.get(key);
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

  @SuppressWarnings("unchecked")
  private long getKeyBucket(K key)
  {
    return keyBucketExtractor != null ? keyBucketExtractor.getBucket(key) : bucket;
  }

}
