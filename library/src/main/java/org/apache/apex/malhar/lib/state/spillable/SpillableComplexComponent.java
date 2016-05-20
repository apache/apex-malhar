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

import org.apache.apex.malhar.lib.state.spillable.Spillable.SpillableComponent;
import org.apache.apex.malhar.lib.utils.serde.Serde;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.Slice;

/**
 * This is a composite component containing spillable data structures. This should be used as
 * a component inside {@link com.datatorrent.api.Operator}s.
 */
public interface SpillableComplexComponent extends Component<OperatorContext>, SpillableComponent
{
  /**
   * This is a method for creating a {@link SpillableArrayList}. This method
   * auto-generates an identifier for the data structure.
   * @param <T> The type of data stored in the {@link SpillableArrayList}.
   * @param bucket The bucket that this {@link SpillableArrayList} will be spilled too.
   * @param serde The Serializer/Deserializer to use for data stored in the {@link SpillableArrayList}.
   * @return A {@link SpillableArrayList}.
   */
  <T> SpillableArrayList<T> newSpillableArrayList(long bucket, Serde<T, Slice> serde);

  /**
   * This is a method for creating a {@link SpillableArrayList}.
   * @param <T> The type of data stored in the {@link SpillableArrayList}.
   * @param identifier The identifier for this {@link SpillableArrayList}.
   * @param bucket The bucket that this {@link SpillableArrayList} will be spilled too.
   * @param serde The Serializer/Deserializer to use for data stored in the {@link SpillableArrayList}.
   * @return A {@link SpillableArrayList}.
   */
  <T> SpillableArrayList<T> newSpillableArrayList(byte[] identifier, long bucket, Serde<T, Slice> serde);

  /**
   * This is a method for creating a {@link SpillableByteMap}. This method
   * auto-generates an identifier for the data structure.
   * @param <K> The type of the keys.
   * @param <V> The type of the values.
   * @param bucket The bucket that this {@link SpillableByteMap} will be spilled too.
   * @param serdeKey The Serializer/Deserializer to use for the map's keys.
   * @param serdeValue The Serializer/Deserializer to use for the map's values.
   * @return A {@link SpillableByteMap}.
   */
  <K, V> SpillableByteMap<K, V> newSpillableByteMap(long bucket, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue);

  /**
   * This is a method for creating a {@link SpillableByteMap}.
   * @param <K> The type of the keys.
   * @param <V> The type of the values.
   * @param identifier The identifier for this {@link SpillableByteMap}.
   * @param bucket The bucket that this {@link SpillableByteMap} will be spilled too.
   * @param serdeKey The Serializer/Deserializer to use for the map's keys.
   * @param serdeValue The Serializer/Deserializer to use for the map's values.
   * @return A {@link SpillableByteMap}.
   */
  <K, V> SpillableByteMap<K, V> newSpillableByteMap(byte[] identifier, long bucket,
      Serde<K, Slice> serdeKey, Serde<V, Slice> serdeValue);

  /**
   * This is a method for creating a {@link SpillableByteArrayListMultimap}. This method
   * auto-generates an identifier for the data structure.
   * @param <K> The type of the keys.
   * @param <V> The type of the values in the map's lists.
   * @param bucket The bucket that this {@link SpillableByteArrayListMultimap} will be spilled too.
   * @param serdeKey The Serializer/Deserializer to use for the map's keys.
   * @param serdeValue The Serializer/Deserializer to use for the values in the map's lists.
   * @return A {@link SpillableByteArrayListMultimap}.
   */
  <K, V> SpillableByteArrayListMultimap<K, V> newSpillableByteArrayListMultimap(long bucket, Serde<K,
      Slice> serdeKey, Serde<V, Slice> serdeValue);

  /**
   * This is a method for creating a {@link SpillableByteArrayListMultimap}.
   * @param <K> The type of the keys.
   * @param <V> The type of the values in the map's lists.
   * @param identifier The identifier for this {@link SpillableByteArrayListMultimap}.
   * @param bucket The bucket that this {@link SpillableByteArrayListMultimap} will be spilled too.
   * @param serdeKey The Serializer/Deserializer to use for the map's keys.
   * @param serdeValue The Serializer/Deserializer to use for the values in the map's lists.
   * @return A {@link SpillableByteArrayListMultimap}.
   */
  <K, V> SpillableByteArrayListMultimap<K, V> newSpillableByteArrayListMultimap(byte[] identifier, long bucket,
      Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue);

  /**
   * This is a method for creating a {@link SpillableByteMultiset}. This method
   * auto-generates an identifier for the data structure.
   * @param <T> The type of the elements.
   * @param bucket The bucket that this {@link SpillableByteMultiset} will be spilled too.
   * @param serde The Serializer/Deserializer to use for data stored in the {@link SpillableByteMultiset}.
   * @return A {@link SpillableByteMultiset}.
   */
  <T> SpillableByteMultiset<T> newSpillableByteMultiset(long bucket, Serde<T, Slice> serde);

  /**
   * This is a method for creating a {@link SpillableByteMultiset}.
   * @param <T> The type of the elements.
   * @param identifier The identifier for this {@link SpillableByteMultiset}.
   * @param bucket The bucket that this {@link SpillableByteMultiset} will be spilled too.
   * @param serde The Serializer/Deserializer to use for data stored in the {@link SpillableByteMultiset}.
   * @return A {@link SpillableByteMultiset}.
   */
  <T> SpillableByteMultiset<T> newSpillableByteMultiset(byte[] identifier, long bucket, Serde<T, Slice> serde);

  /**
   * This is a method for creating a {@link SpillableQueue}. This method
   * auto-generates an identifier for the data structure.
   * @param <T> The type of the data stored in the {@link SpillableQueue}.
   * @param bucket The bucket that this {@link SpillableQueue} will be spilled too.
   * @param serde The Serializer/Deserializer to use for data stored in the {@link SpillableQueue}.
   * @return A {@link SpillableQueue}.
   */
  <T> SpillableQueue<T> newSpillableQueue(long bucket, Serde<T, Slice> serde);

  /**
   * This is a method for creating a {@link SpillableQueue}.
   * @param <T> The type of the data stored in the {@link SpillableQueue}.
   * @param identifier The identifier for this {@link SpillableByteArrayListMultimap}.
   * @param bucket The bucket that this {@link SpillableQueue} will be spilled too.
   * @param serde The Serializer/Deserializer to use for data stored in the {@link SpillableQueue}.
   * @return A {@link SpillableQueue}.
   */
  <T> SpillableQueue<T> newSpillableQueue(byte[] identifier, long bucket, Serde<T, Slice> serde);
}
