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

import java.util.List;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

import com.datatorrent.netlet.util.Slice;

/**
 * The serialize was implemented by this class, the deserialize was inherited from super class
 *
 * @param <T> The type of serializer for item
 */
public class SerdeListSliceWithSerializeBuffer<T> extends SerdeListSlice<T> implements SerToSerializeBuffer<List<T>>
{
  protected SerToSerializeBuffer<T> itemSerTo;
  protected LengthValueBuffer buffer;
  
  private SerdeListSliceWithSerializeBuffer()
  {
    // for Kryo
  }

  public SerdeListSliceWithSerializeBuffer(@NotNull SerToSerializeBuffer<T> serde, LengthValueBuffer buffer)
  {
    super(serde);
    this.itemSerTo = Preconditions.checkNotNull(serde);
    this.buffer = Preconditions.checkNotNull(buffer);
  }

  @Override
  public Slice serialize(List<T> objects)
  {
    serTo(objects, buffer);
    return buffer.toSlice();
  }
  
  @Override
  public void serTo(List<T> objects, SerializeBuffer buffer)
  {
    //For LengthValueBuffer, need to set the size
    if (buffer instanceof LengthValueBuffer) {
      ((LengthValueBuffer)buffer).setObjectLength(objects.size());
    }

    for (T object : objects) {
      itemSerTo.serTo(object, buffer);;
    }
  }

  public void reset()
  {
    buffer.reset();
  }
  
}
