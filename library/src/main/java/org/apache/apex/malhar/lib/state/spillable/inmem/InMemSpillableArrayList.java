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
import java.util.List;
import java.util.ListIterator;

import org.apache.apex.malhar.lib.state.spillable.Spillable;

import com.google.common.collect.Lists;

/**
 * An in memory implementation of the {@link Spillable.SpillableArrayList} interface.
 * @param <T> The type of the data stored in the {@link InMemSpillableArrayList}
 */
public class InMemSpillableArrayList<T> implements Spillable.SpillableArrayList<T>
{
  private List<T> list = Lists.newArrayList();

  @Override
  public int size()
  {
    return list.size();
  }

  @Override
  public boolean isEmpty()
  {
    return list.isEmpty();
  }

  @Override
  public boolean contains(Object o)
  {
    return list.contains(o);
  }

  @Override
  public Iterator<T> iterator()
  {
    return list.iterator();
  }

  @Override
  public Object[] toArray()
  {
    return list.toArray();
  }

  @Override
  public <T1> T1[] toArray(T1[] t1s)
  {
    return list.toArray(t1s);
  }

  @Override
  public boolean add(T t)
  {
    return list.add(t);
  }

  @Override
  public boolean remove(Object o)
  {
    return list.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> collection)
  {
    return list.containsAll(collection);
  }

  @Override
  public boolean addAll(Collection<? extends T> collection)
  {
    return list.addAll(collection);
  }

  @Override
  public boolean addAll(int i, Collection<? extends T> collection)
  {
    return list.addAll(i, collection);
  }

  @Override
  public boolean removeAll(Collection<?> collection)
  {
    return list.removeAll(collection);
  }

  @Override
  public boolean retainAll(Collection<?> collection)
  {
    return list.retainAll(collection);
  }

  @Override
  public void clear()
  {
    list.clear();
  }

  @Override
  public T get(int i)
  {
    return list.get(i);
  }

  @Override
  public T set(int i, T t)
  {
    return list.set(i, t);
  }

  @Override
  public void add(int i, T t)
  {
    list.add(i, t);
  }

  @Override
  public T remove(int i)
  {
    return list.remove(i);
  }

  @Override
  public int indexOf(Object o)
  {
    return list.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o)
  {
    return list.lastIndexOf(o);
  }

  @Override
  public ListIterator<T> listIterator()
  {
    return list.listIterator();
  }

  @Override
  public ListIterator<T> listIterator(int i)
  {
    return list.listIterator(i);
  }

  @Override
  public List<T> subList(int i, int i1)
  {
    return list.subList(i, i1);
  }
}
