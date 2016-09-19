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

import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * This is a factory that is used for Spillable datastructures. This component is used by nesting it inside of an
 * operator and forwarding the appropriate operator callbacks are called on the {@link SpillableComplexComponentImpl}.
 * Spillable datastructures are created by called the appropriate factory methods on the
 * {@link SpillableComplexComponentImpl} in the setup method of an operator.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class SpillableComplexComponentImpl implements SpillableComplexComponent
{
  private List<SpillableComponent> componentList = Lists.newArrayList();

  @NotNull
  private SpillableStateStore store;

  @NotNull
  private SpillableIdentifierGenerator identifierGenerator;

  private SpillableComplexComponentImpl()
  {
    // for kryo
  }

  public SpillableComplexComponentImpl(SpillableStateStore store)
  {
    this(store, new SequentialSpillableIdentifierGenerator());
  }

  public SpillableComplexComponentImpl(SpillableStateStore store, SpillableIdentifierGenerator identifierGenerator)
  {
    this.store = Preconditions.checkNotNull(store);
    this.identifierGenerator = Preconditions.checkNotNull(identifierGenerator);
  }

  public <T> SpillableArrayList<T> newSpillableArrayList(long bucket, Serde<T, Slice> serde)
  {
    SpillableArrayListImpl<T> list = new SpillableArrayListImpl<T>(bucket, identifierGenerator.next(), store, serde);
    componentList.add(list);
    return list;
  }

  public <T> SpillableArrayList<T> newSpillableArrayList(byte[] identifier, long bucket, Serde<T, Slice> serde)
  {
    identifierGenerator.register(identifier);
    SpillableArrayListImpl<T> list = new SpillableArrayListImpl<T>(bucket, identifier, store, serde);
    componentList.add(list);
    return list;
  }

  public <K, V> SpillableByteMap<K, V> newSpillableByteMap(long bucket, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    SpillableByteMapImpl<K, V> map = new SpillableByteMapImpl<K, V>(store, identifierGenerator.next(),
        bucket, serdeKey, serdeValue);
    componentList.add(map);
    return map;
  }

  public <K, V> SpillableByteMap<K, V> newSpillableByteMap(byte[] identifier, long bucket, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    identifierGenerator.register(identifier);
    SpillableByteMapImpl<K, V> map = new SpillableByteMapImpl<K, V>(store, identifier, bucket, serdeKey, serdeValue);
    componentList.add(map);
    return map;
  }

  public <K, V> SpillableByteArrayListMultimap<K, V> newSpillableByteArrayListMultimap(long bucket, Serde<K,
      Slice> serdeKey, Serde<V, Slice> serdeValue)
  {
    SpillableByteArrayListMultimapImpl<K, V> map = new SpillableByteArrayListMultimapImpl<K, V>(store,
        identifierGenerator.next(), bucket, serdeKey, serdeValue);
    componentList.add(map);
    return map;
  }

  public <K, V> SpillableByteArrayListMultimap<K, V> newSpillableByteArrayListMultimap(byte[] identifier, long bucket,
      Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    identifierGenerator.register(identifier);
    SpillableByteArrayListMultimapImpl<K, V> map = new SpillableByteArrayListMultimapImpl<K, V>(store,
        identifier, bucket, serdeKey, serdeValue);
    componentList.add(map);
    return map;
  }

  public <T> SpillableByteMultiset<T> newSpillableByteMultiset(long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  public <T> SpillableByteMultiset<T> newSpillableByteMultiset(byte[] identifier, long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  public <T> SpillableQueue<T> newSpillableQueue(long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  public <T> SpillableQueue<T> newSpillableQueue(byte[] identifier, long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    store.setup(context);
    for (SpillableComponent spillableComponent: componentList) {
      spillableComponent.setup(context);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    store.beginWindow(windowId);
    for (SpillableComponent spillableComponent: componentList) {
      spillableComponent.beginWindow(windowId);
    }
  }

  @Override
  public void endWindow()
  {
    for (SpillableComponent spillableComponent: componentList) {
      spillableComponent.endWindow();
    }
    store.endWindow();
  }

  @Override
  public void teardown()
  {
    for (SpillableComponent spillableComponent: componentList) {
      spillableComponent.teardown();
    }
    store.teardown();
  }

  @Override
  public void beforeCheckpoint(long l)
  {
    store.beforeCheckpoint(l);
  }

  @Override
  public void checkpointed(long l)
  {
    store.checkpointed(l);
  }

  @Override
  public void committed(long l)
  {
    store.committed(l);
  }
}
