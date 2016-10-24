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
package org.apache.apex.malhar.lib.utils.serde;

import java.io.IOException;
import java.util.Collection;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * This is an implementation of {@link Serde} which serializes and deserializes lists.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class CollectionSerde<T, CollectionT extends Collection<T>> implements Serde<CollectionT>
{
  @NotNull
  private Serde<T> serde;

  @NotNull
  private Class<? extends CollectionT> collectionClass;

  //required by sub class
  protected CollectionSerde()
  {
    // for Kryo
  }

  /**
   * Creates a {@link CollectionSerde}.
   * @param serde The {@link Serde} that is used to serialize and deserialize each element of a list.
   */
  public CollectionSerde(@NotNull Serde<T> serde, @NotNull Class<? extends CollectionT> collectionClass /*Class<? extends C1> collectionClass*/ )
  {
    this.serde = Preconditions.checkNotNull(serde);
    this.collectionClass = Preconditions.checkNotNull(collectionClass);
  }

  @Override
  public void serialize(CollectionT objects, SerializationBuffer buffer)
  {
    if (objects.size() == 0) {
      return;
    }
    try {
      buffer.writeInt(objects.size());
    } catch (IOException e) {
      throw new RuntimeException("Not suppose to get this exception.");
    }
    Serde<T> serializer = getItemSerde();
    for (T object : objects) {
      serializer.serialize(object, buffer);
    }
  }

  @Override
  public CollectionT deserialize(byte[] buffer, MutableInt offset, int length)
  {
    int orgOffset = offset.intValue();
    int numElements = GPOUtils.deserializeInt(buffer, offset);
    try {
      CollectionT collection = collectionClass.newInstance();

      for (int index = 0; index < numElements; index++) {
        T object = serde.deserialize(buffer, offset, length - (offset.intValue() - orgOffset));
        collection.add(object);
      }

      return collection;
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }


  public void deserialize(Slice slice, MutableInt offset, CollectionT target)
  {
    MutableInt sliceOffset = new MutableInt(slice.offset + offset.intValue());

    int numElements = GPOUtils.deserializeInt(slice.buffer, sliceOffset);
    sliceOffset.subtract(slice.offset);
    try {

      for (int index = 0; index < numElements; index++) {
        T object = serde.deserialize(slice.buffer, sliceOffset, slice.length);
        target.add(object);
      }

      offset.setValue(sliceOffset.intValue());
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  protected Serde<T> getItemSerde()
  {
    return serde;
  }
}
