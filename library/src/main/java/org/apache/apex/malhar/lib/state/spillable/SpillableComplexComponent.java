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

import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.state.spillable.Spillable.SpillableComponent;
import org.apache.apex.malhar.lib.utils.serde.Serde;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;

/**
 * This is a composite component containing spillable data structures. This should be used as
 * a component inside {@link com.datatorrent.api.Operator}s.
 *
 * @since 3.4.0
 */
public interface SpillableComplexComponent extends Component<OperatorContext>, SpillableComponent,
    Operator.CheckpointNotificationListener
{
  /**
   * This is a method for creating a {@link SpillableList}. This method
   * auto-generates an identifier for the data structure.
   * @param <T> The type of data stored in the {@link SpillableList}.
   * @param bucket The bucket that this {@link SpillableList} will be spilled to.
   * @param serde The Serializer/Deserializer to use for data stored in the {@link SpillableList}.
   * @return A {@link SpillableList}.
   */
  <T> SpillableList<T> newSpillableArrayList(long bucket, Serde<T> serde);

  /**
   * This is a method for creating a {@link SpillableList}.
   * @param <T> The type of data stored in the {@link SpillableList}.
   * @param identifier The identifier for this {@link SpillableList}.
   * @param bucket The bucket that this {@link SpillableList} will be spilled to.
   * @param serde The Serializer/Deserializer to use for data stored in the {@link SpillableList}.
   * @return A {@link SpillableList}.
   */
  <T> SpillableList<T> newSpillableArrayList(byte[] identifier, long bucket, Serde<T> serde);

  /**
   * This is a method for creating a {@link SpillableMap}. This method
   * auto-generates an identifier for the data structure.
   * @param <K> The type of the keys.
   * @param <V> The type of the values.
   * @param bucket The bucket that this {@link SpillableMap} will be spilled to.
   * @param serdeKey The Serializer/Deserializer to use for the map's keys.
   * @param serdeValue The Serializer/Deserializer to use for the map's values.
   * @return A {@link SpillableMap}.
   */
  <K, V> SpillableMap<K, V> newSpillableMap(long bucket, Serde<K> serdeKey,
      Serde<V> serdeValue);

  /**
   * This is a method for creating a {@link SpillableMap}. This method
   * auto-generates an identifier for the data structure.
   * @param <K> The type of the keys.
   * @param <V> The type of the values.
   * @param serdeKey The Serializer/Deserializer to use for the map's keys.
   * @param serdeValue The Serializer/Deserializer to use for the map's values.
   * @param timeExtractor a util object to extract time from key.
   * @return A {@link SpillableMap}.
   */
  <K, V> SpillableMap<K, V> newSpillableMap(Serde<K> serdeKey,
      Serde<V> serdeValue, TimeExtractor<K> timeExtractor);

  /**
   * This is a method for creating a {@link SpillableMap}.
   * @param <K> The type of the keys.
   * @param <V> The type of the values.
   * @param identifier The identifier for this {@link SpillableMap}.
   * @param bucket The bucket that this {@link SpillableMap} will be spilled to.
   * @param serdeKey The Serializer/Deserializer to use for the map's keys.
   * @param serdeValue The Serializer/Deserializer to use for the map's values.
   * @return A {@link SpillableMap}.
   */
  <K, V> SpillableMap<K, V> newSpillableMap(byte[] identifier, long bucket,
      Serde<K> serdeKey, Serde<V> serdeValue);

  /**
   * This is a method for creating a {@link SpillableMap}.
   * @param <K> The type of the keys.
   * @param <V> The type of the values.
   * @param identifier The identifier for this {@link SpillableMap}.
   * @param serdeKey The Serializer/Deserializer to use for the map's keys.
   * @param serdeValue The Serializer/Deserializer to use for the map's values.
   * @param timeExtractor a util object to extract time from key.
   * @return A {@link SpillableMap}.
   */
  <K, V> SpillableMap<K, V> newSpillableMap(byte[] identifier,
      Serde<K> serdeKey, Serde<V> serdeValue, TimeExtractor<K> timeExtractor);

  /**
   * This is a method for creating a {@link SpillableListMultimap}. This method
   * auto-generates an identifier for the data structure.
   * @param <K> The type of the keys.
   * @param <V> The type of the values in the map's lists.
   * @param bucket The bucket that this {@link SpillableListMultimap} will be spilled to.
   * @param serdeKey The Serializer/Deserializer to use for the map's keys.
   * @param serdeValue The Serializer/Deserializer to use for the values in the map's lists.
   * @return A {@link SpillableListMultimap}.
   */
  <K, V> SpillableListMultimap<K, V> newSpillableArrayListMultimap(long bucket, Serde<K> serdeKey, Serde<V> serdeValue);

  /**
   * This is a method for creating a {@link SpillableListMultimap}.
   * @param <K> The type of the keys.
   * @param <V> The type of the values in the map's lists.
   * @param identifier The identifier for this {@link SpillableListMultimap}.
   * @param bucket The bucket that this {@link SpillableListMultimap} will be spilled to.
   * @param serdeKey The Serializer/Deserializer to use for the map's keys.
   * @param serdeValue The Serializer/Deserializer to use for the values in the map's lists.
   * @return A {@link SpillableListMultimap}.
   */
  <K, V> SpillableListMultimap<K, V> newSpillableArrayListMultimap(byte[] identifier, long bucket,
      Serde<K> serdeKey,
      Serde<V> serdeValue);

  /**
   * This is a method for creating a {@link SpillableSetMultimap}.
   * @param <K> The type of the keys.
   * @param <V> The type of the values in the map's lists.
   * @param bucket The bucket that this {@link SpillableSetMultimap} will be spilled to.
   * @param serdeKey The Serializer/Deserializer to use for the map's keys.
   * @param serdeValue The Serializer/Deserializer to use for the values in the map's lists.
   * @return A {@link SpillableSetMultimap}.
   */
  <K, V> SpillableSetMultimap<K, V> newSpillableSetMultimap(long bucket, Serde<K> serdeKey, Serde<V> serdeValue);

  /**
   * This is a method for creating a {@link SpillableSetMultimap}.
   * @param <K> The type of the keys.
   * @param <V> The type of the values in the map's lists.
   * @param bucket The bucket that this {@link SpillableSetMultimap} will be spilled to.
   * @param serdeKey The Serializer/Deserializer to use for the map's keys.
   * @param serdeValue The Serializer/Deserializer to use for the values in the map's lists.
   * @param timeExtractor a util object to extract time from key.
   * @return A {@link SpillableSetMultimap}.
   */
  <K, V> SpillableSetMultimap<K, V> newSpillableSetMultimap(long bucket, Serde<K> serdeKey,
      Serde<V> serdeValue, TimeExtractor<K> timeExtractor);

  /**
   * This is a method for creating a {@link SpillableMultiset}. This method
   * auto-generates an identifier for the data structure.
   * @param <T> The type of the elements.
   * @param bucket The bucket that this {@link SpillableMultiset} will be spilled to.
   * @param serde The Serializer/Deserializer to use for data stored in the {@link SpillableMultiset}.
   * @return A {@link SpillableMultiset}.
   */
  <T> SpillableMultiset<T> newSpillableMultiset(long bucket, Serde<T> serde);

  /**
   * This is a method for creating a {@link SpillableMultiset}.
   * @param <T> The type of the elements.
   * @param identifier The identifier for this {@link SpillableMultiset}.
   * @param bucket The bucket that this {@link SpillableMultiset} will be spilled to.
   * @param serde The Serializer/Deserializer to use for data stored in the {@link SpillableMultiset}.
   * @return A {@link SpillableMultiset}.
   */
  <T> SpillableMultiset<T> newSpillableMultiset(byte[] identifier, long bucket, Serde<T> serde);

  /**
   * This is a method for creating a {@link SpillableQueue}. This method
   * auto-generates an identifier for the data structure.
   * @param <T> The type of the data stored in the {@link SpillableQueue}.
   * @param bucket The bucket that this {@link SpillableQueue} will be spilled to.
   * @param serde The Serializer/Deserializer to use for data stored in the {@link SpillableQueue}.
   * @return A {@link SpillableQueue}.
   */
  <T> SpillableQueue<T> newSpillableQueue(long bucket, Serde<T> serde);

  /**
   * This is a method for creating a {@link SpillableQueue}.
   * @param <T> The type of the data stored in the {@link SpillableQueue}.
   * @param identifier The identifier for this {@link SpillableListMultimap}.
   * @param bucket The bucket that this {@link SpillableQueue} will be spilled to.
   * @param serde The Serializer/Deserializer to use for data stored in the {@link SpillableQueue}.
   * @return A {@link SpillableQueue}.
   */
  <T> SpillableQueue<T> newSpillableQueue(byte[] identifier, long bucket, Serde<T> serde);

  SpillableStateStore getStore();
}
