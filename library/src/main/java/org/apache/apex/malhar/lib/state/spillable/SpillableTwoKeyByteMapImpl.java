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
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerdeCollectionSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdePairSlice;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * An implementation of {@link SpillableTwoKeyByteMap}
 * @param <K1> The type of the first key
 * @param <K2> The type of the second key
 * @param <V> The type of values.
 */
public class SpillableTwoKeyByteMapImpl<K1, K2, V> implements Spillable.SpillableTwoKeyByteMap<K1, K2, V>, Spillable.SpillableComponent,
    Serializable
{
  private SpillableStateStore store;
  private SpillableByteMapImpl<Pair<K1, K2>, V> map;
  private SpillableByteMapImpl<K1, Set<K2>> keyMap;

  private SpillableTwoKeyByteMapImpl()
  {
    // for kryo
  }

  /**
   * Creates a {@link SpillableByteMapImpl}.
   * @param store The {@link SpillableStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableByteMapImpl}.
   * @param bucket The Id of the bucket used to store this
   * {@link SpillableByteMapImpl} in the provided {@link SpillableStateStore}.
   * @param serdeKey1 The {@link Serde} to use when serializing and deserializing the first key.
   * @param serdeKey2 The {@link Serde} to use when serializing and deserializing the second key.
   * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
   */
  public SpillableTwoKeyByteMapImpl(SpillableStateStore store, byte[] identifier, long bucket,
      Serde<K1, Slice> serdeKey1, Serde<K2, Slice> serdeKey2, Serde<V, Slice> serdeValue)
  {
    this.store = store;
    map = new SpillableByteMapImpl<>(store, identifier, bucket, new SerdePairSlice<>(serdeKey1, serdeKey2), serdeValue);
    keyMap = new SpillableByteMapImpl<>(store, identifier, bucket, serdeKey1, new SerdeCollectionSlice<K2, Set<K2>>(serdeKey2, (Class<TreeSet<K2>>)(Class)TreeSet.class));
  }

  public SpillableStateStore getStore()
  {
    return this.store;
  }

  @Override
  public V get(K1 key1, K2 key2)
  {
    return map.get(new ImmutablePair<>(key1, key2));
  }

  @Override
  public void put(K1 key1, K2 key2, V value)
  {
    map.put(new ImmutablePair<>(key1, key2), value);
    Set<K2> keys = keyMap.get(key1);
    if (keys == null) {
      keys = new TreeSet<>();
    }
    keys.add(key2);
    keyMap.put(key1, keys);
  }

  @Override
  public void remove(K1 key1)
  {
    Set<K2> keys = keyMap.get(key1);
    if (keys != null) {
      for (K2 key2 : keys) {
        map.remove(new ImmutablePair<>(key1, key2));
      }
    }
    keyMap.remove(key1);
  }

  @Override
  public void remove(K1 key1, K2 key2)
  {
    Set<K2> keys = keyMap.get(key1);
    if (keys != null) {
      map.remove(new ImmutablePair<>(key1, key2));
      keys.remove(key2);
      if (keys.isEmpty()) {
        keyMap.remove(key1);
      } else {
        keyMap.put(key1, keys);
      }
    }
  }

  @Override
  public long size()
  {
    return map.size();
  }

  @Override
  public Iterator<Map.Entry<K2, V>> iterator(final K1 key1)
  {
    final Set<K2> keys2 = keyMap.get(key1);

    return new Iterator<Map.Entry<K2, V>>()
    {
      private Iterator<K2> listIterator = keys2 == null ? null : keys2.iterator();
      private K2 lastKey2;

      @Override
      public boolean hasNext()
      {
        return listIterator != null && listIterator.hasNext();
      }

      @Override
      public Map.Entry<K2, V> next()
      {
        if (listIterator == null) {
          throw new NoSuchElementException();
        } else {
          lastKey2 = listIterator.next();
          return new AbstractMap.SimpleEntry<>(lastKey2, map.get(new ImmutablePair<>(key1, lastKey2)));
        }
      }

      @Override
      public void remove()
      {
        SpillableTwoKeyByteMapImpl.this.remove(key1, lastKey2);
      }
    };
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    map.setup(context);
    keyMap.setup(context);
  }

  @Override
  public void teardown()
  {
    keyMap.teardown();
    map.teardown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    map.beginWindow(windowId);
    keyMap.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    keyMap.endWindow();
    map.endWindow();
  }
}
