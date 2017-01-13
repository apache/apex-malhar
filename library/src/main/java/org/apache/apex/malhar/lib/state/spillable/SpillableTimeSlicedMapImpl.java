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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.BucketedState;
import org.apache.apex.malhar.lib.state.managed.KeyBucketExtractor;
import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.utils.serde.AffixKeyValueSerdeManager;
import org.apache.apex.malhar.lib.utils.serde.BufferSlice;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * Spillable implementation of map over SpillableTimeStateStore
 * @param <K> Key type
 * @param <V> Value type
 */
@InterfaceStability.Evolving
@DefaultSerializer(FieldSerializer.class)
public class SpillableTimeSlicedMapImpl<K,V> implements Spillable.SpillableMap<K, V>, Spillable.SpillableComponent,
    Serializable
{
  private transient WindowBoundedMapCache<K, V> cache = new WindowBoundedMapCache<>();
  private transient Input tmpInput = new Input();
  private transient long timeIncrement;
  private long time = System.currentTimeMillis();

  private TimeExtractor timeExtractor;
  private KeyBucketExtractor<K> keyBucketExtractor;

  @NotNull
  private SpillableTimeStateStore store;

  private long bucket;

  private int size = 0;

  protected AffixKeyValueSerdeManager<K, V> keyValueSerdeManager;

  private SpillableTimeSlicedMapImpl()
  {
    //for kryo
  }

  /**
   * Creats a {@link SpillableTimeSlicedMapImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableTimeSlicedMapImpl}.
   * @param bucket The Id of the bucket used to store this
   * {@link SpillableTimeSlicedMapImpl} in the provided {@link SpillableTimeStateStore}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
   */
  public SpillableTimeSlicedMapImpl(SpillableTimeStateStore store, byte[] identifier, long bucket, Serde<K> serdeKey,
      Serde<V> serdeValue)
  {
    this.store = Preconditions.checkNotNull(store);
    this.bucket = bucket;
    keyValueSerdeManager = new AffixKeyValueSerdeManager<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
  }

  /**
   * Creats a {@link SpillableTimeSlicedMapImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableTimeSlicedMapImpl}.
   * @param bucket The Id of the bucket used to store this
   * {@link SpillableTimeSlicedMapImpl} in the provided {@link SpillableTimeStateStore}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
   * @param timeExtractor Extract time from the each element and use it to decide where the data goes
   */
  public SpillableTimeSlicedMapImpl(SpillableTimeStateStore store, byte[] identifier, long bucket, Serde<K> serdeKey,
      Serde<V> serdeValue, TimeExtractor timeExtractor)
  {
    this.store = Preconditions.checkNotNull(store);
    this.bucket = bucket;
    this.timeExtractor = timeExtractor;
    keyValueSerdeManager = new AffixKeyValueSerdeManager<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
  }

  /**
   * Creats a {@link SpillableTimeSlicedMapImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableTimeSlicedMapImpl}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
   * @param timeExtractor Extract time from the each element and use it to decide where the data goes
   * @param keyBucketExtractor Extract bucket id from the each element and use it to decide where the data goes
   */
  public SpillableTimeSlicedMapImpl(SpillableTimeStateStore store, byte[] identifier, Serde<K> serdeKey,
      Serde<V> serdeValue, @NotNull TimeExtractor<K> timeExtractor, @NotNull KeyBucketExtractor<K> keyBucketExtractor)
  {
    this.store = Preconditions.checkNotNull(store);
    keyValueSerdeManager = new AffixKeyValueSerdeManager<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
    this.timeExtractor = timeExtractor;
    this.keyBucketExtractor = keyBucketExtractor;
  }

  /**
   * Creats a {@link SpillableTimeSlicedMapImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableTimeSlicedMapImpl}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
   * @param keyBucketExtractor Extract bucket id from the each element and use it to decide where the data goes
   */
  public SpillableTimeSlicedMapImpl(SpillableTimeStateStore store, byte[] identifier, Serde<K> serdeKey,
      Serde<V> serdeValue, @NotNull KeyBucketExtractor keyBucketExtractor)
  {
    this.store = Preconditions.checkNotNull(store);
    keyValueSerdeManager = new AffixKeyValueSerdeManager<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
    this.keyBucketExtractor = keyBucketExtractor;
  }

  /**
   * Creats a {@link SpillableTimeSlicedMapImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableTimeSlicedMapImpl}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
   */
  public SpillableTimeSlicedMapImpl(SpillableTimeStateStore store, byte[] identifier, Serde<K> serdeKey,
      Serde<V> serdeValue)
  {
    this.store = Preconditions.checkNotNull(store);
    keyValueSerdeManager = new AffixKeyValueSerdeManager<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
  }

  public SpillableTimeStateStore getStore()
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

    Slice valSlice = store.getSync(getKeyBucket(key), keyValueSerdeManager.serializeDataKey(key, false));

    if (valSlice == null || valSlice == BucketedState.EXPIRED || valSlice.length == 0) {
      return null;
    }

    tmpInput.setBuffer(valSlice.buffer, valSlice.offset, valSlice.length);
    return keyValueSerdeManager.deserializeValue(tmpInput);
  }

  /**
   * Returns the future using which the value is obtained. If the value is in cache then returns the immediateFuture.
   * @param o given object
   * @return Future<V>
   */
  public Future<V> getAsync(Object o)
  {
    K key = (K)o;
    V val = cache.get(key);

    if (val != null) {
      return Futures.immediateFuture(val);
    }

    Future<Slice> valSlice = store.getAsync(getKeyBucket(key), keyValueSerdeManager.serializeDataKey(key, false));
    return new DeserializeValueFuture(valSlice, new Input(), keyValueSerdeManager);
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
  public Set<Map.Entry<K, V>> entrySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    timeIncrement = context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
      context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
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
    for (K key: cache.getChangedKeys()) {
      store.put(getKeyBucket(key), getTimeBucket(cache.get(key)), keyValueSerdeManager.serializeDataKey(key, true),
          keyValueSerdeManager.serializeValue(cache.get(key)));
    }

    for (K key: cache.getRemovedKeys()) {
      store.put(getKeyBucket(key), time, keyValueSerdeManager.serializeDataKey(key, true), BufferSlice.EMPTY_SLICE);
    }
    cache.endWindow();
    keyValueSerdeManager.resetReadBuffer();
    time += timeIncrement;
  }

  @Override
  public void teardown()
  {
  }

  private long getTimeBucket(V value)
  {
    return timeExtractor != null ? timeExtractor.getTime(value) : time;
  }

  private long getKeyBucket(K key)
  {
    return keyBucketExtractor != null ? keyBucketExtractor.getBucket(key) : bucket;
  }

  /**
   * Converts the Future<Slice> to Future<V> and this is needed for getAsync
   */
  public class DeserializeValueFuture implements Future<V>
  {
    private Future<Slice> slice;
    private Input tmpInput;
    private AffixKeyValueSerdeManager<K, V> keyValueSerdeManager;

    public DeserializeValueFuture(Future<Slice> slice, Input tmpInput, AffixKeyValueSerdeManager keyValueSerdeManager)
    {
      this.slice = slice;
      this.tmpInput = tmpInput;
      this.keyValueSerdeManager = keyValueSerdeManager;
    }

    @Override
    public boolean cancel(boolean b)
    {
      return slice.cancel(b);
    }

    @Override
    public boolean isCancelled()
    {
      return slice.isCancelled();
    }

    @Override
    public boolean isDone()
    {
      return slice.isDone();
    }

    /**
     * Converts the single element into the list.
     * @return the list of values
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public V get() throws InterruptedException, ExecutionException
    {
      Slice valSlice = slice.get();
      if (valSlice == null || valSlice == BucketedState.EXPIRED || valSlice.length == 0) {
        return null;
      }

      tmpInput.setBuffer(valSlice.buffer, valSlice.offset, valSlice.length);
      return keyValueSerdeManager.deserializeValue(tmpInput);
    }

    @Override
    public V get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException
    {
      throw new UnsupportedOperationException();
    }
  }
}
