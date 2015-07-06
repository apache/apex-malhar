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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;

public class SerdeListGPOMutable implements Serde
{
  public static final SerdeListGPOMutable INSTANCE = new SerdeListGPOMutable();

  private GPOByteArrayList bytes = new GPOByteArrayList();

  private SerdeListGPOMutable()
  {
  }

  @Override
  public synchronized byte[] serializeObject(Object object)
  {
    @SuppressWarnings("unchecked")
    List<GPOMutable> mutables = (List<GPOMutable>) object;

    if(mutables.isEmpty()) {
      return GPOUtils.serializeInt(0);
    }

    FieldsDescriptor fd = mutables.get(0).getFieldDescriptor();

    bytes.add(SerdeFieldsDescriptor.INSTANCE.serializeObject(fd));

    for(int index = 0;
        index < mutables.size();
        index++) {
      bytes.add(GPOUtils.serialize(mutables.get(index), bytes));
    }

    byte[] byteArray = bytes.toByteArray();
    bytes.clear();
    bytes.add(GPOUtils.serializeInt(byteArray.length));
    bytes.add(byteArray);
    byteArray = bytes.toByteArray();
    bytes.clear();
    return byteArray;
  }

  @Override
  public synchronized Object deserializeObject(byte[] object, MutableInt offset)
  {
    int length = GPOUtils.deserializeInt(object, offset);
    int startIndex = offset.intValue();

    if(length == 0) {
      return new ArrayList<GPOMutable>();
    }

    FieldsDescriptor fd =
    (FieldsDescriptor) SerdeFieldsDescriptor.INSTANCE.deserializeObject(object, offset);

    List<GPOMutable> mutables = Lists.newArrayList();
    while(startIndex + length > offset.intValue()) {
      GPOMutable value = GPOUtils.deserialize(fd, object, offset);
      mutables.add(value);
    }

    return mutables;
  }
}
