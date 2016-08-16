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

import java.lang.reflect.Array;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.base.Preconditions;

import com.datatorrent.lib.appdata.gpo.GPOUtils;

public class ArraySerde<T> implements Serde<T[]>
{
  protected Serde<T> itemSerde;
  protected boolean keepArraySize = true;
  protected Class<T> itemType;

  protected ArraySerde()
  {
  }

  /**
   * Serializer and Deserializer need different constructor, so use static factory method to wrap.
   * The ArraySerde returned by newSerializer can only used for serialization
   */
  public static <T> ArraySerde<T> newSerializer(Serde<T> itemSerde)
  {
    return new ArraySerde<T>(Preconditions.checkNotNull(itemSerde));
  }

  public static <T> ArraySerde<T> newSerde(Serde<T> itemSerde, Class<T> itemType)
  {
    return new ArraySerde<T>(Preconditions.checkNotNull(itemSerde), Preconditions.checkNotNull(itemType));
  }

  protected ArraySerde(Serde<T> itemSerde)
  {
    this.itemSerde = itemSerde;
  }

  protected ArraySerde(Serde<T> itemSerde, Class<T> itemType)
  {
    this.itemSerde = itemSerde;
    this.itemType = itemType;
  }

  @Override
  public void serialize(T[] objects, SerializationBuffer buffer)
  {
    if (objects.length == 0) {
      return;
    }

    if (keepArraySize) {
      buffer.write(objects.length);
    }

    Serde<T> serializer = getItemSerde();
    for (T object : objects) {
      serializer.serialize(object, buffer);
    }
  }

  protected Serde<T> getItemSerde()
  {
    return itemSerde;
  }

  @Override
  public T[] deserialize(byte[] buffer, MutableInt offset, int length)
  {
    int numOfElements = GPOUtils.deserializeInt(buffer, offset);
    if (numOfElements <= 0) {
      throw new IllegalArgumentException(
          "The length of the array is less than or equal to zero. length: " + numOfElements);
    }

    T[] array = createObjectArray(numOfElements);

    for (int index = 0; index < numOfElements; ++index) {
      array[index] = getItemSerde().deserialize(buffer, offset, length);
    }
    return array;
  }

  @SuppressWarnings("unchecked")
  protected T[] createObjectArray(int length)
  {
    return (T[])Array.newInstance(itemType, length);
  }
}
