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

import java.util.List;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.lang3.mutable.MutableInt;

public class SerdeListPrimitiveTest
{
  @Test
  public void simpleSerdeTest()
  {
    GPOByteArrayList bal = new GPOByteArrayList();

    List<Object> primitiveList = Lists.newArrayList();
    primitiveList.add((Boolean) true);
    primitiveList.add((Byte) ((byte) 5));
    primitiveList.add((Short) ((short) 16000));
    primitiveList.add((Integer) 25000000);
    primitiveList.add((Long) 5000000000L);
    primitiveList.add('a');
    primitiveList.add("tim is the coolest");

    byte[] plBytes = SerdeListPrimitive.INSTANCE.serializeObject(primitiveList);

    bal.add(new byte[15]);
    bal.add(plBytes);
    bal.add(new byte[13]);

    @SuppressWarnings("unchecked")
    List<Object> newPrimitiveList = (List<Object>) SerdeListPrimitive.INSTANCE.deserializeObject(bal.toByteArray(), new MutableInt(15));

    Assert.assertEquals(primitiveList, newPrimitiveList);
  }
}
