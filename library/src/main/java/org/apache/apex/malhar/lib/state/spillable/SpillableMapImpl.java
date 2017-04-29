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
import org.apache.apex.malhar.lib.state.managed.ManagedTimeUnifiedStateImpl;
import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.utils.serde.AffixKeyValueSerdeManager;
import org.apache.apex.malhar.lib.utils.serde.BufferSlice;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;

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
public class SpillableMapImpl<K, V> implements Spillable.SpillableMap<K, V>, Spillable.SpillableComponent,
    Serializable
{
  private static final long serialVersionUID = 4552547110215784584L;
  private transient WindowBoundedMapCache<K, V> cache = new WindowBoundedMapCache<>();
  private transient Input tmpInput = new Input();

  private TimeExtractor<K> timeExtractor;

  @NotNull
  private SpillableStateStore store;

  private long bucket;

  private int size = 0;

  protected AffixKeyValueSerdeManager<K, V> keyValueSerdeManager;

  private SpillableMapImpl()
  {
    //for kryo
  }

  /**
   * Creats a {@link SpillableMapImpl}.
   * @param store The {@link SpillableStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableMapImpl}.
   * @param bucket The Id of the bucket used to store this
   * {@link SpillableMapImpl} in the provided {@link SpillableStateStore}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
   */
  public SpillableMapImpl(SpillableStateStore store, byte[] identifier, long bucket, Serde<K> serdeKey,
      Serde<V> serdeValue)
  {
    this.store = Preconditions.checkNotNull(store);
    this.bucket = bucket;
    keyValueSerdeManager = new AffixKeyValueSerdeManager<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
  }

  /**
   * Creats a {@link SpillableMapImpl}.
   * @param store The {@link SpillableStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableMapImpl}.
   * {@link SpillableMapImpl} in the provided {@link SpillableStateStore}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
   * @param timeExtractor Extract time from the each element and use it to decide where the data goes
   */
  public SpillableMapImpl(SpillableStateStore store, byte[] identifier, Serde<K> serdeKey,
      Serde<V> serdeValue, TimeExtractor<K> timeExtractor)
  {
    this.store = Preconditions.checkNotNull(store);
    keyValueSerdeManager = new AffixKeyValueSerdeManager<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
    this.timeExtractor = timeExtractor;
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
    K key = (K)o;

    if (cache.getRemovedKeys().contains(key)) {
      return null;
    }

    V val = cache.get(key);

    if (val != null) {
      return val;
    }

    Slice valSlice = store.getSync(getBucketTimeOrId(key), keyValueSerdeManager.serializeDataKey(key, false));

    if (valSlice == null || valSlice == BucketedState.EXPIRED || valSlice.length == 0) {
      return null;
    }

    tmpInput.setBuffer(valSlice.buffer, valSlice.offset, valSlice.length);
    return keyValueSerdeManager.deserializeValue(tmpInput);
  }


  @Override
  public V put(K k, V v)
  {
    V value = get(k);

    if (value == null) {
      size++;
    }

    cache.put(k, v);

    return value;
  }

  @Override
  public V remove(Object o)
  {
    V value = get(o);

    if (value != null) {
      size--;
    }

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
    store.ensureBucket(bucket);
    keyValueSerdeManager.setup(store, bucket);
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    boolean isTimeUnifiedStore = (store instanceof ManagedTimeUnifiedStateImpl);
    for (K key: cache.getChangedKeys()) {
      //the getBucket() returned in fact is time, the bucket assign then assigned the bucketId
      long timeOrBucketId = bucket;
      long bucketId = timeOrBucketId;
      if (isTimeUnifiedStore) {
        timeOrBucketId = getBucketTimeOrId(key);
        bucketId = ((ManagedTimeUnifiedStateImpl)store).getTimeBucketAssigner().getTimeBucket(timeOrBucketId);
      }
      keyValueSerdeManager.updateBuffersForBucketChange(bucketId);
      store.put(timeOrBucketId, keyValueSerdeManager.serializeDataKey(key, true),
          keyValueSerdeManager.serializeValue(cache.get(key)));
    }

    for (K key: cache.getRemovedKeys()) {
      long timeOrBucketId = bucket;
      long bucketId = timeOrBucketId;
      if (isTimeUnifiedStore) {
        timeOrBucketId = getBucketTimeOrId(key);
        bucketId = ((ManagedTimeUnifiedStateImpl)store).getTimeBucketAssigner().getTimeBucket(timeOrBucketId);
      }
      keyValueSerdeManager.updateBuffersForBucketChange(bucketId);
      store.put(timeOrBucketId, keyValueSerdeManager.serializeDataKey(key, true), BufferSlice.EMPTY_SLICE);
    }
    cache.endWindow();
    keyValueSerdeManager.resetReadBuffer();
  }

  @Override
  public void teardown()
  {
  }

  /**
   *
   * @param key
   * @return The bucket time for time unified store or bucket id for store with fixed bucket
   */
  private long getBucketTimeOrId(K key)
  {
    return timeExtractor != null ? timeExtractor.getTime(key) : bucket;
  }
}
