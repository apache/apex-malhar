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

import java.util.Map;

import org.apache.apex.malhar.lib.appdata.schemas.Type;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Maps;

/**
 * TODO: this class can move to Malhar. put in Megh for implementing TOP/BOTTOM dimension computation.
 *
 *
 * @since 3.4.0
 */
public class SerdeMapPrimitive  implements Serde
{
  public static final SerdeMapPrimitive INSTANCE = new SerdeMapPrimitive();

  private final GPOByteArrayList bytes = new GPOByteArrayList();

  private SerdeMapPrimitive()
  {
  }

  @Override
  public synchronized byte[] serializeObject(Object object)
  {
    @SuppressWarnings("unchecked")
    Map<Object, Object> primitiveMap = (Map<Object, Object>)object;

    for (Map.Entry<Object, Object> entry : primitiveMap.entrySet() ) {
      serializePrimitive(entry.getKey(), bytes);
      serializePrimitive(entry.getValue(), bytes);
    }

    byte[] serializedBytes = bytes.toByteArray();
    bytes.clear();
    bytes.add(GPOUtils.serializeInt(serializedBytes.length));
    bytes.add(serializedBytes);
    serializedBytes = bytes.toByteArray();
    bytes.clear();
    return serializedBytes;
  }

  protected void serializePrimitive(Object object, GPOByteArrayList bytes)
  {
    Type type = Type.CLASS_TO_TYPE.get(object.getClass());

    if (type == null || type == Type.OBJECT) {
      throw new IllegalArgumentException("Cannot serialize objects of class " + object.getClass());
    }

    bytes.add(GPOUtils.serializeInt(type.ordinal()));
    GPOType gpoType = GPOType.GPO_TYPE_ARRAY[type.ordinal()];
    bytes.add(gpoType.serialize(object));
  }

  @Override
  public synchronized Object deserializeObject(byte[] objectBytes, MutableInt offset)
  {
    int length = GPOUtils.deserializeInt(objectBytes, offset);
    int startIndex = offset.intValue();

    Map<Object, Object> primitiveMap = Maps.newHashMap();

    while (startIndex + length > offset.intValue()) {
      int typeOrdinal = GPOUtils.deserializeInt(objectBytes, offset);
      GPOType gpoType = GPOType.GPO_TYPE_ARRAY[typeOrdinal];
      Object key = gpoType.deserialize(objectBytes, offset);

      typeOrdinal = GPOUtils.deserializeInt(objectBytes, offset);
      gpoType = GPOType.GPO_TYPE_ARRAY[typeOrdinal];
      Object value = gpoType.deserialize(objectBytes, offset);
      primitiveMap.put(key, value);
    }

    return primitiveMap;
  }
}

