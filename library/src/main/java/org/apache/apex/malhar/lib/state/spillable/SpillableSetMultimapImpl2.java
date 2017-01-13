
package org.apache.apex.malhar.lib.state.spillable;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.managed.AbstractManagedStateImpl;
import org.apache.apex.malhar.lib.state.managed.TimeBucketAssigner;
import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.utils.serde.AffixKeyValueSerdeManager;
import org.apache.apex.malhar.lib.utils.serde.AffixSerde;
import org.apache.apex.malhar.lib.utils.serde.IntSerde;
import org.apache.apex.malhar.lib.utils.serde.PairSerde;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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
public class SpillableSetMultimapImpl2<K, V> implements Spillable.SpillableSetMultimap<K, V>,
    Spillable.SpillableComponent
{

  private static class FixedTimeExtractor<V> implements TimeExtractor<V>
  {

    private long fixedTime;

    private FixedTimeExtractor(long fixedTime)
    {
      this.fixedTime = fixedTime;
    }

    private FixedTimeExtractor()
    {
      // For kryo
    }

    @Override
    public long getTime(V v)
    {
      return fixedTime;
    }

  }

  private static class MapTimeExtractor<V> implements TimeExtractor<Pair<Integer, V>>
  {

    TimeExtractor<V> delegator;

    private MapTimeExtractor()
    {
      //for kryo only
    }

    private MapTimeExtractor(TimeExtractor<V> delegator)
    {
      this.delegator = delegator;
    }

    @Override
    public long getTime(Pair<Integer, V> integerVPair)
    {
      return delegator.getTime(integerVPair.getValue());
    }
  }

  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final byte[] META_KEY_SUFFIX = new byte[]{(byte)0, (byte)0, (byte)0};

  private TimeBucketAssigner assigner;

  // LRU cache for key and latest time of values for the key
  private Cache<K, Long> keyToLatestTime = CacheBuilder.newBuilder().build();

  private transient WindowBoundedMapCache<K, SpillableSetImpl<V>> cache = new WindowBoundedMapCache<>();

  @NotNull
  private SpillableMapImpl2<K, Pair<Integer, V>> map;
  private SpillableStateStore store;
  private Serde<V> valueSerde;
  private transient List<SpillableSetImpl<V>> removedSets = new ArrayList<>();

  private TimeExtractor<V> timeExtractorFromValue = null;
  private AffixKeyValueSerdeManager<K, V> keyValueSerdeManager;
  private transient Context.OperatorContext context;

  private SpillableSetMultimapImpl2()
  {
    // for kryo
  }


  /**
   * Creates a {@link SpillableSetMultimapImpl}.
   * @param store The {@link SpillableStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableSetMultimapImpl}.
   * {@link SpillableSetMultimapImpl} in the provided {@link SpillableStateStore}.
   * @param keySerde The {@link Serde} to use when serializing and deserializing keys.
   * @param valueSerde The {@link Serde} to use when serializing and deserializing values.
   * @param timeExtractorFromValue The {@link TimeExtractor} to be used to retrieve time from key
   */
  public SpillableSetMultimapImpl2(SpillableStateStore store, byte[] identifier,
      Serde<K> keySerde,
      Serde<V> valueSerde,
      TimeExtractor<V> timeExtractorFromValue)
  {
    this.store = Preconditions.checkNotNull(store);
    this.valueSerde = Preconditions.checkNotNull(valueSerde);
    keyValueSerdeManager = new AffixKeyValueSerdeManager<K, V>(META_KEY_SUFFIX, identifier, Preconditions.checkNotNull(keySerde), valueSerde);
    this.timeExtractorFromValue = timeExtractorFromValue;
    if (!(store instanceof AbstractManagedStateImpl)) {
      throw new IllegalArgumentException("Store should be AbsractManagedStateImpl to get the time bucket assigner");
    }
    this.assigner = ((AbstractManagedStateImpl)store).getTimeBucketAssigner();

    map = new SpillableMapImpl2<>(store, identifier, new AffixSerde<>(null, keySerde, META_KEY_SUFFIX), new PairSerde<>(new IntSerde(), valueSerde),
        new MapTimeExtractor<>(timeExtractorFromValue));
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
      long keyTime = -1;
      Pair<Integer, V> meta;

      meta = map.get(key);

      if (timeExtractorFromValue != null) {
        Long t = keyToLatestTime.getIfPresent(key);
        if (t != null) {
          keyTime = t;
        }
      }

      if (meta == null) {
        return null;
      }

      Slice keyPrefix = keyValueSerdeManager.serializeDataKey(key, false);
      spillableSet = new SpillableSetImpl<>(keyPrefix.toByteArray(), store, valueSerde, new FixedTimeExtractor(keyTime));
      spillableSet.setSize(meta.getLeft());
      spillableSet.setHead(meta.getRight());
      spillableSet.setup(context);
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
      map.put((K)key, new ImmutablePair<>(0, spillableSet.getHead()));
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
    Pair<Integer, V> meta = map.get((K)key);
    return meta != null && meta.getLeft() > 0;
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

    long eventTime = timeExtractorFromValue.getTime(value);
    if (spillableSet == null) {
      spillableSet = new SpillableSetImpl<>(keyValueSerdeManager.serializeDataKey(key, true).toByteArray(), store, valueSerde, new FixedTimeExtractor(eventTime));
      spillableSet.setup(context);
      cache.put(key, spillableSet);
    } else {
      Long latestForKey = keyToLatestTime.getIfPresent(key);
      if (latestForKey != null && latestForKey < eventTime) {
        if (!assigner.isInSameBucket(latestForKey, eventTime)) {
          // move the set to new bucket
          SpillableSetImpl<V> newSpillableSet = new SpillableSetImpl<>(keyValueSerdeManager.serializeDataKey(key, true).toByteArray(), store, valueSerde, new FixedTimeExtractor(eventTime));
          spillableSet.addAll(spillableSet);
        }
        keyToLatestTime.put(key, eventTime);
      }
    }
    return spillableSet.add(value);
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
    this.context = context;
    map.setup(context);
    keyValueSerdeManager.setup(store, 0);
  }

  @Override
  public void beginWindow(long windowId)
  {
    map.beginWindow(windowId);
    keyValueSerdeManager.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    for (K key: cache.getChangedKeys()) {

      SpillableSetImpl<V> spillableSet = cache.get(key);
      spillableSet.endWindow();

      map.put(key, new ImmutablePair<>(spillableSet.size(), spillableSet.getHead()));
    }

    for (SpillableSetImpl removedSet : removedSets) {
      removedSet.endWindow();
    }

    cache.endWindow();
    map.endWindow();

    keyValueSerdeManager.resetReadBuffer();
  }

  @Override
  public void teardown()
  {
    map.teardown();
  }
}

