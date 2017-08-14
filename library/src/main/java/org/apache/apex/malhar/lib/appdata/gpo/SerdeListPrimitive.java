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
package org.apache.apex.malhar.lib.appdata.gpo;

import java.util.List;

import org.apache.apex.malhar.lib.appdata.schemas.Type;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Lists;

/**
 * @since 3.1.0
 */

public class SerdeListPrimitive implements Serde
{
  public static final SerdeListPrimitive INSTANCE = new SerdeListPrimitive();

  private final GPOByteArrayList bytes = new GPOByteArrayList();

  private SerdeListPrimitive()
  {
  }

  @Override
  public synchronized byte[] serializeObject(Object object)
  {
    @SuppressWarnings("unchecked")
    List<Object> primitives = (List<Object>)object;

    for (int index = 0; index < primitives.size(); index++) {
      Object primitive = primitives.get(index);
      Type type = Type.CLASS_TO_TYPE.get(primitive.getClass());

      if (type == null || type == Type.OBJECT) {
        throw new IllegalArgumentException("Cannot serialize objects of class " + primitive.getClass());
      }

      bytes.add(GPOUtils.serializeInt(type.ordinal()));
      GPOType gpoType = GPOType.GPO_TYPE_ARRAY[type.ordinal()];
      bytes.add(gpoType.serialize(primitive));
    }

    byte[] serializedBytes = bytes.toByteArray();
    bytes.clear();
    bytes.add(GPOUtils.serializeInt(serializedBytes.length));
    bytes.add(serializedBytes);
    serializedBytes = bytes.toByteArray();
    bytes.clear();
    return serializedBytes;
  }

  @Override
  public synchronized Object deserializeObject(byte[] object, MutableInt offset)
  {
    int length = GPOUtils.deserializeInt(object, offset);
    int startIndex = offset.intValue();

    List<Object> listPrimitives = Lists.newArrayList();

    while (startIndex + length > offset.intValue()) {
      int typeOrdinal = GPOUtils.deserializeInt(object, offset);
      GPOType gpoType = GPOType.GPO_TYPE_ARRAY[typeOrdinal];
      Object primitive = gpoType.deserialize(object, offset);
      listPrimitives.add(primitive);
    }

    return listPrimitives;
  }
}
