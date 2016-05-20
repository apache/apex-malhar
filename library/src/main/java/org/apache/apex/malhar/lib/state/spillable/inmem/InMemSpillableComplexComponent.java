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

import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponent;
import org.apache.apex.malhar.lib.utils.serde.Serde;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * An in memory implementation {@link SpillableComplexComponent}
 */
public class InMemSpillableComplexComponent implements SpillableComplexComponent
{
  @Override
  public <T> SpillableArrayList<T> newSpillableArrayList(long bucket, Serde<T, Slice> serde)
  {
    return new InMemSpillableArrayList<>();
  }

  @Override
  public <T> SpillableArrayList<T> newSpillableArrayList(byte[] identifier, long bucket,
      Serde<T, Slice> serde)
  {
    return new InMemSpillableArrayList<>();
  }

  @Override
  public <K, V> SpillableByteMap<K, V> newSpillableByteMap(long bucket, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> SpillableByteMap<K, V> newSpillableByteMap(byte[] identifier, long bucket,
      Serde<K, Slice> serdeKey, Serde<V, Slice> serdeValue)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> SpillableByteArrayListMultimap<K, V> newSpillableByteArrayListMultimap(long bucket,
      Serde<K, Slice> serdeKey, Serde<V, Slice> serdeValue)
  {
    return new InMemSpillableByteArrayListMultimap<>();
  }

  @Override
  public <K, V> SpillableByteArrayListMultimap<K, V> newSpillableByteArrayListMultimap(byte[] identifier,
      long bucket, Serde<K, Slice> serdeKey, Serde<V, Slice> serdeValue)
  {
    return  new InMemSpillableByteArrayListMultimap<>();
  }

  @Override
  public <T> SpillableByteMultiset<T> newSpillableByteMultiset(long bucket, Serde<T, Slice> serde)
  {
    return new InMemMultiset<>();
  }

  @Override
  public <T> SpillableByteMultiset<T> newSpillableByteMultiset(byte[] identifier, long bucket,
      Serde<T, Slice> serde)
  {
    return new InMemMultiset<>();
  }

  @Override
  public <T> SpillableQueue<T> newSpillableQueue(long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> SpillableQueue<T> newSpillableQueue(byte[] identifier, long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void teardown()
  {
  }
}
