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
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.SetMultimap;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context.OperatorContext;

/**
 * This is a marker interface for a spillable data structure.
 *
 * @since 3.4.0
 */
public interface Spillable
{
  /**
   * This represents a spillable {@link java.util.List}. Users that receive an
   * implementation of this interface don't need to worry about propagating operator call-backs
   * to the data structure.
   *
   * @param <T> The type of the data stored in the {@link SpillableList}.
   */
  interface SpillableList<T> extends List<T>
  {
  }

  /**
   * This represents a spillable {@link java.util.Set}. Users that receive an
   * implementation of this interface don't need to worry about propagating operator call-backs
   * to the data structure.
   *
   * @param <T> The type of the data stored in the {@link SpillableSet}.
   */
  interface SpillableSet<T> extends Set<T>
  {
  }

  /**
   * This represents a spillable {@link java.util.Map}. Implementations make
   * some assumptions about serialization and equality. Consider two keys K1 and K2. The assumption is
   * that K1.equals(K2) should be consistent with K1.toByteArray().equals(K2.toByteArray()). Users that receive an
   * implementation of this interface don't need to worry about propagating operator call-backs
   * to the data structure.
   *
   * @param <K> The type of the keys.
   * @param <V> The type of the values.
   */
  interface SpillableMap<K, V> extends Map<K, V>
  {
  }

  /**
   * This represents a spillable {@link com.google.common.collect.ListMultimap} implementation. Implementations make
   * some assumptions about serialization and equality. Consider two keys K1 and K2. The assumption is
   * that K1.equals(K2) should be consistent with K1.toByteArray().equals(K2.toByteArray()). Users that receive an
   * implementation of this interface don't need to worry about propagating operator call-backs
   * to the data structure.
   *
   * @param <K> The type of the keys.
   * @param <V> The type of the values.
   */
  interface SpillableListMultimap<K, V> extends ListMultimap<K, V>
  {
  }

  /**
   * This represents a spillable {@link com.google.common.collect.SetMultimap} implementation. Implementations make
   * some assumptions about serialization and equality. Consider two keys K1 and K2. The assumption is
   * that K1.equals(K2) should be consistent with K1.toByteArray().equals(K2.toByteArray()). Users that receive an
   * implementation of this interface don't need to worry about propagating operator call-backs
   * to the data structure.
   *
   * @param <K> The type of the keys.
   * @param <V> The type of the values.
   */
  interface SpillableSetMultimap<K, V> extends SetMultimap<K, V>
  {
  }

  /**
   * This represents a spillable {@link com.google.common.collect.Multiset} implementation. Implementations make
   * some assumptions about serialization and equality. Consider two elements T1 and T2. The assumption is
   * that T1.equals(T2) should be consistent with T1.toByteArray().equals(T2.toByteArray()). Users that receive an
   * implementation of this interface don't need to worry about propagating operator call-backs to the data structure.
   *
   * @param <T> The type of the data stored in the set.
   */
  interface SpillableMultiset<T> extends Multiset<T>
  {
  }

  /**
   * This represents a spillable {@link java.util.Queue} implementation. Users that receive an
   * implementation of this interface don't need to worry about propagating operator call-backs
   * to the data structure.
   *
   * @param <T> The type of the data stored in the queue.
   */
  interface SpillableQueue<T> extends Queue<T>
  {
  }

  /**
   * This represents a spillable data structure that needs to be aware of the operator
   * callbacks. All concrete or abstract implementations of spillable data structures
   * should implement this interface. A user working with an implementation of this interface needs
   * to make sure that the {@link com.datatorrent.api.Operator} call-backs are propagated to it.
   */
  interface SpillableComponent extends Component<OperatorContext>, Spillable, WindowListener
  {
  }
}
