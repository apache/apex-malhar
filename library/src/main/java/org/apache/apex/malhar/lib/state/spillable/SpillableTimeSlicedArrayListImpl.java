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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.Future;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.utils.serde.CollectionSerde;
import org.apache.apex.malhar.lib.utils.serde.IntSerde;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context;

/**
 * Implementation of Spillable array list over SpillableTimeStateStore
 * @param <T>
 */
@InterfaceStability.Evolving
@DefaultSerializer(FieldSerializer.class)
public class SpillableTimeSlicedArrayListImpl<T> implements Spillable.SpillableList<T>, Spillable.SpillableComponent
{
  public static final int DEFAULT_BATCH_SIZE = 1000;

  private int batchSize = DEFAULT_BATCH_SIZE;
  private long bucketId;
  private byte[] prefix;

  @NotNull
  private SpillableTimeStateStore store;
  @NotNull
  private Serde<T> serde;
  @NotNull
  private SpillableTimeSlicedMapImpl<Integer, List<T>> map;

  private int size;
  private int numBatches;

  private SpillableTimeSlicedArrayListImpl()
  {
    //for kryo
  }

  public SpillableTimeStateStore getStore()
  {
    return store;
  }

  /**
   * Creates a {@link SpillableTimeSlicedArrayListImpl}.
   * @param bucketId The Id of the bucket used to store this
   * {@link SpillableTimeSlicedArrayListImpl} in the provided {@link SpillableTimeStateStore}.
   * @param prefix The Id of this {@link SpillableTimeSlicedArrayListImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param serde The {@link Serde} to use when serializing and deserializing data.
   */
  public SpillableTimeSlicedArrayListImpl(long bucketId, @NotNull byte[] prefix,
      @NotNull SpillableTimeStateStore store, @NotNull Serde<T> serde)
  {
    this.bucketId = bucketId;
    this.prefix = Preconditions.checkNotNull(prefix);
    this.store = Preconditions.checkNotNull(store);
    this.serde = Preconditions.checkNotNull(serde);

    map = new SpillableTimeSlicedMapImpl<>(store, prefix, bucketId, new IntSerde(),
      new CollectionSerde<T, List<T>>(serde, (Class)ArrayList.class));
  }

  /**
   * Creates a {@link SpillableTimeSlicedArrayListImpl}.
   * @param bucketId The Id of the bucket used to store this
   * {@link SpillableTimeSlicedArrayListImpl} in the provided {@link SpillableTimeStateStore}.
   * @param prefix The Id of this {@link SpillableTimeSlicedArrayListImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param serde The {@link SpillableTimeStateStore} in which to spill to.
   * @param timeExtractor Extract time from the each element and use it to decide where the data goes
   */
  public SpillableTimeSlicedArrayListImpl(long bucketId, @NotNull byte[] prefix,
      @NotNull SpillableTimeStateStore store, @NotNull Serde<T> serde, TimeExtractor timeExtractor)
  {
    this.bucketId = bucketId;
    this.prefix = Preconditions.checkNotNull(prefix);
    this.store = Preconditions.checkNotNull(store);
    this.serde = Preconditions.checkNotNull(serde);

    if (timeExtractor != null) {
      map = new SpillableTimeSlicedMapImpl<>(store, prefix, bucketId, new IntSerde(),
        new CollectionSerde<T, List<T>>(serde, (Class)ArrayList.class), new ArrayListTimeExtractor(timeExtractor));
    } else {
      map = new SpillableTimeSlicedMapImpl<>(store, prefix, bucketId, new IntSerde(),
        new CollectionSerde<T, List<T>>(serde, (Class)ArrayList.class));
    }

  }

  /**
   * Creates a {@link SpillableTimeSlicedArrayListImpl}.
   * @param bucketId The Id of the bucket used to store this
   * {@link SpillableTimeSlicedArrayListImpl} in the provided {@link SpillableTimeStateStore}.
   * @param prefix The Id of this {@link SpillableTimeSlicedArrayListImpl}.
   * @param store The {@link SpillableTimeStateStore} in which to spill to.
   * @param serde The {@link Serde} to use when serializing and deserializing data.
   * @param batchSize When spilled to a {@link SpillableTimeStateStore} data is stored in a batch. This determines the
   *                  number of elements a batch will contain when it's spilled. Having small batches will increase
   *                  the number of keys stored by your {@link SpillableTimeStateStore} but will improve random reads and
   *                  writes. Increasing the batch size will improve sequential read and write speed.
   */
  public SpillableTimeSlicedArrayListImpl(long bucketId, @NotNull byte[] prefix,
      @NotNull SpillableTimeStateStore store, @NotNull Serde<T> serde, int batchSize)
  {
    this(bucketId, prefix, store, serde);

    Preconditions.checkArgument(this.batchSize > 0);
    this.batchSize = batchSize;
  }

  public void setSize(int size)
  {
    Preconditions.checkArgument(size >= 0);
    this.size = size;
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
  public boolean contains(Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<T> iterator()
  {
    return new Iterator<T>()
    {
      private int index = 0;

      @Override
      public boolean hasNext()
      {
        return index < size;
      }

      @Override
      public T next()
      {
        return get(index++);
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public Object[] toArray()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T1> T1[] toArray(T1[] t1s)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(T t)
  {
    Preconditions.checkArgument((size() + 1) > 0);

    int batchIndex = (size / batchSize);

    List<T> batch = null;

    if (batchIndex == numBatches) {
      batch = Lists.newArrayListWithCapacity(batchSize);
      numBatches++;
    } else {
      batch = map.get(batchIndex);
    }

    batch.add(t);

    size++;
    map.put(batchIndex, batch);
    return true;
  }

  public boolean add(T t, long timeBucket)
  {
    Preconditions.checkArgument((size() + 1) > 0);

    int batchIndex = (size / batchSize);

    List<T> batch = null;

    if (batchIndex == numBatches) {
      batch = Lists.newArrayListWithCapacity(batchSize);
      numBatches++;
    } else {
      batch = map.get(batchIndex);
    }

    batch.add(t);

    size++;
    map.put(batchIndex, batch);
    return true;
  }

  @Override
  public boolean remove(Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> collection)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends T> collection)
  {
    for (T element: collection) {
      add(element);
    }

    return true;
  }

  @Override
  public boolean addAll(int i, Collection<? extends T> collection)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> collection)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> collection)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public T get(int i)
  {
    if (!(i < size)) {
      throw new IndexOutOfBoundsException();
    }

    int batchIndex = i / batchSize;
    int batchOffset = i % batchSize;

    List<T> batch = map.get(batchIndex);
    return batch.get(batchOffset);
  }

  public Future<List<T>> getAsync()
  {
    return map.getAsync(0);
  }

  @Override
  public T set(int i, T t)
  {
    if (!(i < size)) {
      throw new IndexOutOfBoundsException();
    }

    int batchIndex = i / batchSize;
    int batchOffset = i % batchSize;

    List<T> batch = map.get(batchIndex);
    T old = batch.get(batchOffset);
    batch.set(batchOffset, t);
    map.put(batchIndex, batch);
    return old;
  }

  @Override
  public void add(int i, T t)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public T remove(int i)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int indexOf(Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int lastIndexOf(Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListIterator<T> listIterator()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListIterator<T> listIterator(int i)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<T> subList(int i, int i1)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    store.ensureBucket(bucketId);
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
    map.endWindow();
  }

  @Override
  public void teardown()
  {
    map.teardown();
  }

  public class ArrayListTimeExtractor implements TimeExtractor<List>
  {
    private TimeExtractor timeExtractor;

    public ArrayListTimeExtractor(TimeExtractor timeExtractor)
    {
      this.timeExtractor = timeExtractor;
    }

    @Override
    public long getTime(List list)
    {
      return timeExtractor.getTime(list.get(list.size() - 1));
    }
  }
}
