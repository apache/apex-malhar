/*
 * Copyright (c) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.appdata.gpo;

import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;

public class SerdeFieldsDescriptor implements Serde
{
  public static final SerdeFieldsDescriptor INSTANCE = new SerdeFieldsDescriptor();

  private GPOByteArrayList bal = new GPOByteArrayList();

  private SerdeFieldsDescriptor()
  {
  }

  @Override
  public synchronized byte[] serializeObject(Object object)
  {
    FieldsDescriptor fd = (FieldsDescriptor) object;

    for(Map.Entry<String, Type> entry: fd.getFieldToType().entrySet()) {
      bal.add(GPOUtils.serializeInt(entry.getValue().ordinal()));
      bal.add(GPOUtils.serializeString(entry.getKey()));
    }

    byte[] serializedBytes = bal.toByteArray();
    bal.clear();
    bal.add(GPOUtils.serializeInt(serializedBytes.length));
    bal.add(serializedBytes);
    serializedBytes = bal.toByteArray();
    bal.clear();
    return serializedBytes;
  }

  @Override
  public synchronized Object deserializeObject(byte[] object, MutableInt offset)
  {
    Map<String, Type> fieldToType = Maps.newHashMap();

    int length = GPOUtils.deserializeInt(object, offset);
    int startIndex = offset.intValue();

    while(startIndex + length > offset.intValue()) {
      Type type = Type.values()[GPOUtils.deserializeInt(object, offset)];
      String value = GPOUtils.deserializeString(object, offset);

      fieldToType.put(value, type);
    }

    return new FieldsDescriptor(fieldToType);
  }
}
