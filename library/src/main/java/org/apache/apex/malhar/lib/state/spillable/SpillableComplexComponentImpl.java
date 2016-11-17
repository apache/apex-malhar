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
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;

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

  /**
   * need to make sure all the buckets are created during setup.
   */
  protected transient Set<Long> bucketIds = Sets.newHashSet();

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

  @Override
  public <T> SpillableList<T> newSpillableArrayList(long bucket, Serde<T> serde)
  {
    SpillableArrayListImpl<T> list = new SpillableArrayListImpl<>(bucket, identifierGenerator.next(), store, serde);
    componentList.add(list);
    return list;
  }

  @Override
  public <T> SpillableList<T> newSpillableArrayList(byte[] identifier, long bucket, Serde<T> serde)
  {
    identifierGenerator.register(identifier);
    SpillableArrayListImpl<T> list = new SpillableArrayListImpl<>(bucket, identifier, store, serde);
    bucketIds.add(bucket);
    componentList.add(list);
    return list;
  }

  @Override
  public <K, V> SpillableMap<K, V> newSpillableMap(long bucket, Serde<K> serdeKey,
      Serde<V> serdeValue)
  {
    SpillableMapImpl<K, V> map = new SpillableMapImpl<>(store, identifierGenerator.next(),
        bucket, serdeKey, serdeValue);
    bucketIds.add(bucket);
    componentList.add(map);
    return map;
  }

  @Override
  public <K, V> SpillableMap<K, V> newSpillableMap(byte[] identifier, long bucket, Serde<K> serdeKey,
      Serde<V> serdeValue)
  {
    identifierGenerator.register(identifier);
    SpillableMapImpl<K, V> map = new SpillableMapImpl<>(store, identifier, bucket, serdeKey, serdeValue);
    bucketIds.add(bucket);
    componentList.add(map);
    return map;
  }

  @Override
  public <K, V> SpillableMap<K, V> newSpillableMap(Serde<K> serdeKey,
      Serde<V> serdeValue, TimeExtractor<K> timeExtractor)
  {
    SpillableMapImpl<K, V> map = new SpillableMapImpl<>(store, identifierGenerator.next(), serdeKey, serdeValue, timeExtractor);
    componentList.add(map);
    return map;
  }

  @Override
  public <K, V> SpillableMap<K, V> newSpillableMap(byte[] identifier, Serde<K> serdeKey,
      Serde<V> serdeValue, TimeExtractor<K> timeExtractor)
  {
    identifierGenerator.register(identifier);
    SpillableMapImpl<K, V> map = new SpillableMapImpl<>(store, identifier, serdeKey, serdeValue, timeExtractor);
    componentList.add(map);
    return map;
  }

  @Override
  public <K, V> SpillableListMultimap<K, V> newSpillableArrayListMultimap(long bucket, Serde<K> serdeKey, Serde<V> serdeValue)
  {
    SpillableArrayListMultimapImpl<K, V> map = new SpillableArrayListMultimapImpl<>(store,
        identifierGenerator.next(), bucket, serdeKey, serdeValue);
    bucketIds.add(bucket);
    componentList.add(map);
    return map;
  }

  @Override
  public <K, V> SpillableListMultimap<K, V> newSpillableArrayListMultimap(byte[] identifier, long bucket,
      Serde<K> serdeKey,
      Serde<V> serdeValue)
  {
    identifierGenerator.register(identifier);
    SpillableArrayListMultimapImpl<K, V> map = new SpillableArrayListMultimapImpl<>(store,
        identifier, bucket, serdeKey, serdeValue);
    bucketIds.add(bucket);
    componentList.add(map);
    return map;
  }

  @Override
  public <K, V> SpillableSetMultimap<K, V> newSpillableSetMultimap(long bucket, Serde<K> serdeKey, Serde<V> serdeValue)
  {
    SpillableSetMultimapImpl<K, V> map = new SpillableSetMultimapImpl<>(store,
        identifierGenerator.next(), bucket, serdeKey, serdeValue);
    bucketIds.add(bucket);
    componentList.add(map);
    return map;
  }

  @Override
  public <K, V> SpillableSetMultimap<K, V> newSpillableSetMultimap(long bucket, Serde<K> serdeKey,
      Serde<V> serdeValue, TimeExtractor<K> timeExtractor)
  {
    SpillableSetMultimapImpl<K, V> map = new SpillableSetMultimapImpl<>(store,
        identifierGenerator.next(), bucket, serdeKey, serdeValue, timeExtractor);
    componentList.add(map);
    return map;
  }

  @Override
  public <T> SpillableMultiset<T> newSpillableMultiset(long bucket, Serde<T> serde)
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  @Override
  public <T> SpillableMultiset<T> newSpillableMultiset(byte[] identifier, long bucket, Serde<T> serde)
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  @Override
  public <T> SpillableQueue<T> newSpillableQueue(long bucket, Serde<T> serde)
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  @Override
  public <T> SpillableQueue<T> newSpillableQueue(byte[] identifier, long bucket, Serde<T> serde)
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    store.setup(context);

    //ensure buckets created.
    for (long bucketId : bucketIds) {
      store.ensureBucket(bucketId);
    }

    //the bucket ids are only for setup. We don't need bucket ids during run time.
    bucketIds.clear();

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

  public SpillableStateStore getStore()
  {
    return store;
  }
}
