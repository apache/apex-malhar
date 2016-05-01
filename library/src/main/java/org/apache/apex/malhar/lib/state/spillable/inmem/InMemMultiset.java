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
import java.util.Iterator;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.apex.malhar.lib.state.spillable.Spillable;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.collect.HashMultiset;

/**
 * An in memory implementation of the {@link Spillable.SpillableByteMultiset} interface.
 * @param <T> The type of the data stored in the {@link InMemMultiset}
 */
public class InMemMultiset<T> implements Spillable.SpillableByteMultiset<T>
{
  @FieldSerializer.Bind(JavaSerializer.class)
  private HashMultiset<T> multiset = HashMultiset.create();

  @Override
  public int count(@Nullable Object element)
  {
    return multiset.count(element);
  }

  @Override
  public int add(@Nullable T element, int occurrences)
  {
    return multiset.add(element, occurrences);
  }

  @Override
  public int remove(@Nullable Object element, int occurrences)
  {
    return multiset.remove(element, occurrences);
  }

  @Override
  public int setCount(T element, int count)
  {
    return multiset.setCount(element, count);
  }

  @Override
  public boolean setCount(T element, int oldCount, int newCount)
  {
    return multiset.setCount(element, oldCount, newCount);
  }

  @Override
  public Set<T> elementSet()
  {
    return multiset.elementSet();
  }

  @Override
  public Set<Entry<T>> entrySet()
  {
    return multiset.entrySet();
  }

  @Override
  public Iterator<T> iterator()
  {
    return multiset.iterator();
  }

  @Override
  public Object[] toArray()
  {
    return multiset.toArray();
  }

  @Override
  public <T1> T1[] toArray(T1[] t1s)
  {
    return multiset.toArray(t1s);
  }

  @Override
  public int size()
  {
    return multiset.size();
  }

  @Override
  public boolean isEmpty()
  {
    return multiset.isEmpty();
  }

  @Override
  public boolean contains(@Nullable Object element)
  {
    return multiset.contains(element);
  }

  @Override
  public boolean containsAll(Collection<?> es)
  {
    return multiset.containsAll(es);
  }

  @Override
  public boolean addAll(Collection<? extends T> collection)
  {
    return multiset.addAll(collection);
  }

  @Override
  public boolean add(T element)
  {
    return multiset.add(element);
  }

  @Override
  public boolean remove(@Nullable Object element)
  {
    return multiset.remove(element);
  }

  @Override
  public boolean removeAll(Collection<?> c)
  {
    return multiset.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c)
  {
    return multiset.retainAll(c);
  }

  @Override
  public void clear()
  {
    multiset.clear();
  }
}
