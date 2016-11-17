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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;

/**
 * A Spillable implementation of {@link List} backed by a {@link SpillableStateStore}.
 * @param <T> The type of object stored in the {@link SpillableSetImpl}.
 *
 * @since 3.5.0
 */
@DefaultSerializer(FieldSerializer.class)
@InterfaceStability.Evolving
public class SpillableSetImpl<T> implements Spillable.SpillableSet<T>, Spillable.SpillableComponent
{
  private static class ListNode<T>
  {
    ListNode()
    {
    }

    ListNode(boolean valid, T next)
    {
      this.valid = valid;
      this.next = next;
    }

    boolean valid;
    T next;
  }

  public static class ListNodeSerde<T> implements Serde<ListNode<T>>
  {
    private Serde<T> serde;

    public ListNodeSerde(@NotNull Serde<T> serde)
    {
      this.serde = Preconditions.checkNotNull(serde);
    }

    @Override
    public void serialize(ListNode<T> object, Output output)
    {
      output.writeBoolean(object.valid);
      serde.serialize(object.next, output);
    }

    @Override
    public ListNode<T> deserialize(Input input)
    {
      ListNode<T> result = new ListNode<>();
      result.valid = input.readBoolean();
      result.next = serde.deserialize(input);
      return result;
    }
  }

  @NotNull
  private SpillableMapImpl<T, ListNode<T>> map;

  private T head;
  private int size;

  private SpillableSetImpl()
  {
    //for kryo
  }

  public SpillableStateStore getStore()
  {
    return map.getStore();
  }

  /**
   * Creates a {@link SpillableSetImpl}.
   * @param bucketId The Id of the bucket used to store this
   * {@link SpillableSetImpl} in the provided {@link SpillableStateStore}.
   * @param prefix The Id of this {@link SpillableSetImpl}.
   * @param store The {@link SpillableStateStore} in which to spill to.
   * @param serde The {@link Serde} to use when serializing and deserializing data.
   */
  public SpillableSetImpl(long bucketId, @NotNull byte[] prefix,
      @NotNull SpillableStateStore store,
      @NotNull Serde<T> serde)
  {
    map = new SpillableMapImpl<>(Preconditions.checkNotNull(store), prefix, bucketId, serde, new ListNodeSerde<>(serde));
  }

  /**
   * Creates a {@link SpillableSetImpl}.
   * {@link SpillableSetImpl} in the provided {@link SpillableStateStore}.
   * @param prefix The Id of this {@link SpillableSetImpl}.
   * @param store The {@link SpillableStateStore} in which to spill to.
   * @param serde The {@link Serde} to use when serializing and deserializing data.
   * @param timeExtractor Extract time from the each element and use it to decide where the data goes.
   */
  public SpillableSetImpl(@NotNull byte[] prefix,
      @NotNull SpillableStateStore store,
      @NotNull Serde<T> serde,
      @NotNull TimeExtractor<T> timeExtractor)
  {
    map = new SpillableMapImpl<>(Preconditions.checkNotNull(store), prefix, serde, new ListNodeSerde<>(serde), timeExtractor);
  }

  public void setSize(int size)
  {
    Preconditions.checkArgument(size >= 0);
    this.size = size;
  }

  public void setHead(T head)
  {
    Preconditions.checkNotNull(head);
    this.head = head;
  }

  public T getHead()
  {
    return head;
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
    T t = (T)o;
    ListNode<T> node = map.get(t);
    return node != null && node.valid;
  }

  @Override
  public Iterator<T> iterator()
  {
    return new Iterator<T>()
    {
      T cur = head;
      T prev = null;

      @Override
      public boolean hasNext()
      {
        while (cur != null) {
          ListNode<T> node = map.get(cur);
          if (node.valid) {
            return true;
          }
          if (cur.equals(node.next)) {
            break;
          } else {
            cur = node.next;
          }
        }
        return false;
      }

      @Override
      public T next()
      {
        while (cur != null) {
          ListNode<T> node = map.get(cur);
          try {
            if (node.valid) {
              prev = cur;
              return prev;
            }
          } finally {
            if (cur.equals(node.next)) {
              cur = null;
            } else {
              cur = node.next;
            }
          }
        }
        throw new NoSuchElementException();
      }

      @Override
      public void remove()
      {
        ListNode<T> node = map.get(prev);
        node.valid = false;
        map.put(prev, node);
        size--;
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
    ListNode<T> node = map.get(t);
    if (node == null) {
      map.put(t, new ListNode<>(true, head == null ? t : head));
      head = t;
      size++;
      return true;
    } else if (!node.valid) {
      node.valid = true;
      map.put(t, node);
      size++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean remove(Object o)
  {
    T t = (T)o;
    ListNode<T> node = map.get(t);
    if (node == null || !node.valid) {
      return false;
    } else {
      node.valid = false;
      map.put(t, node);
      size--;
      return true;
    }
  }

  @Override
  public boolean containsAll(Collection<?> collection)
  {
    for (Object item : collection) {
      if (!contains(item)) {
        return false;
      }
    }
    return true;
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
    Iterator<T> it = iterator();
    while (it.hasNext()) {
      it.next();
      it.remove();
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
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
}
