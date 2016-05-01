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
package org.apache.apex.malhar.lib.state.spillable.inmem;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.apex.malhar.lib.state.spillable.Spillable;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

/**
 * An in memory implementation of the {@link Spillable.SpillableByteArrayListMultimap} interface.
 * @param <K> The type of the keys stored in the {@link InMemSpillableByteArrayListMultimap}
 * @param <V> The type of the values stored in the {@link InMemSpillableByteArrayListMultimap}
 */
public class InMemSpillableByteArrayListMultimap<K, V> implements Spillable.SpillableByteArrayListMultimap<K, V>
{
  @FieldSerializer.Bind(JavaSerializer.class)
  private ListMultimap<K, V> multimap = ArrayListMultimap.create();

  @Override
  public List<V> get(@Nullable K key)
  {
    return multimap.get(key);
  }

  @Override
  public Set<K> keySet()
  {
    return multimap.keySet();
  }

  @Override
  public Multiset<K> keys()
  {
    return multimap.keys();
  }

  @Override
  public Collection<V> values()
  {
    return multimap.values();
  }

  @Override
  public Collection<Map.Entry<K, V>> entries()
  {
    return multimap.entries();
  }

  @Override
  public List<V> removeAll(@Nullable Object key)
  {
    return multimap.removeAll(key);
  }

  @Override
  public void clear()
  {
    multimap.clear();
  }

  @Override
  public int size()
  {
    return multimap.size();
  }

  @Override
  public boolean isEmpty()
  {
    return multimap.isEmpty();
  }

  @Override
  public boolean containsKey(@Nullable Object key)
  {
    return multimap.containsKey(key);
  }

  @Override
  public boolean containsValue(@Nullable Object value)
  {
    return multimap.containsValue(value);
  }

  @Override
  public boolean containsEntry(@Nullable Object key, @Nullable Object value)
  {
    return multimap.containsEntry(key, value);
  }

  @Override
  public boolean put(@Nullable K key, @Nullable V value)
  {
    return multimap.put(key, value);
  }

  @Override
  public boolean remove(@Nullable Object key, @Nullable Object value)
  {
    return multimap.remove(key, value);
  }

  @Override
  public boolean putAll(@Nullable K key, Iterable<? extends V> values)
  {
    return multimap.putAll(key, values);
  }

  @Override
  public boolean putAll(Multimap<? extends K, ? extends V> m)
  {
    return multimap.putAll(m);
  }

  @Override
  public List<V> replaceValues(K key, Iterable<? extends V> values)
  {
    return multimap.replaceValues(key, values);
  }

  @Override
  public Map<K, Collection<V>> asMap()
  {
    return multimap.asMap();
  }
}
