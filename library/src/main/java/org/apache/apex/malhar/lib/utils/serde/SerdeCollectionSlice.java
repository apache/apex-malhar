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

import java.util.Collection;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * This is an implementation of {@link Serde} which serializes and deserializes lists.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class SerdeCollectionSlice<T, CollectionT extends Collection<T>> implements Serde<CollectionT, Slice>
{
  @NotNull
  private Serde<T, Slice> serde;

  @NotNull
  private Class<? extends CollectionT> collectionClass;

  private SerdeCollectionSlice()
  {
    // for Kryo
  }

  /**
   * Creates a {@link SerdeCollectionSlice}.
   * @param serde The {@link Serde} that is used to serialize and deserialize each element of a list.
   */
  public SerdeCollectionSlice(@NotNull Serde<T, Slice> serde, @NotNull Class<? extends CollectionT> collectionClass)
  {
    this.serde = Preconditions.checkNotNull(serde);
    this.collectionClass = Preconditions.checkNotNull(collectionClass);
  }

  @Override
  public Slice serialize(CollectionT objects)
  {
    Slice[] slices = new Slice[objects.size()];

    int size = 4;

    int index = 0;
    for (T object : objects) {
      Slice slice = serde.serialize(object);
      slices[index++] = slice;
      size += slice.length;
    }

    byte[] bytes = new byte[size];
    int offset = 0;

    byte[] sizeBytes = GPOUtils.serializeInt(objects.size());
    System.arraycopy(sizeBytes, 0, bytes, offset, 4);
    offset += 4;

    for (index = 0; index < slices.length; index++) {
      Slice slice = slices[index];
      System.arraycopy(slice.buffer, slice.offset, bytes, offset, slice.length);
      offset += slice.length;
    }

    return new Slice(bytes);
  }

  @Override
  public CollectionT deserialize(Slice slice, MutableInt offset)
  {
    MutableInt sliceOffset = new MutableInt(slice.offset + offset.intValue());

    int numElements = GPOUtils.deserializeInt(slice.buffer, sliceOffset);
    sliceOffset.subtract(slice.offset);
    try {
      CollectionT collection = collectionClass.newInstance();

      for (int index = 0; index < numElements; index++) {
        T object = serde.deserialize(slice, sliceOffset);
        collection.add(object);
      }

      offset.setValue(sliceOffset.intValue());
      return collection;
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public CollectionT deserialize(Slice slice)
  {
    return deserialize(slice, new MutableInt(0));
  }
}
