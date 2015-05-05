/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.dimensions.AggregateEvent.EventKey;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class AggregateEventTest
{
  @Test
  public void eventKeyEqualsHashCodeTest()
  {
    Map<String, Type> fieldToTypeA = Maps.newHashMap();
    fieldToTypeA.put("a", Type.LONG);
    fieldToTypeA.put("b", Type.STRING);

    FieldsDescriptor fdA = new FieldsDescriptor(fieldToTypeA);

    GPOMutable gpoA = new GPOMutable(fdA);
    gpoA.setField("a", 1L);
    gpoA.setField("b", "hello");

    EventKey eventKeyA = new EventKey(1, 1, 1, gpoA);

    Map<String, Type> fieldToTypeB = Maps.newHashMap();
    fieldToTypeB.put("a", Type.LONG);
    fieldToTypeB.put("b", Type.STRING);

    FieldsDescriptor fdB = new FieldsDescriptor(fieldToTypeB);

    GPOMutable gpoB = new GPOMutable(fdB);
    gpoB.setField("a", 1L);
    gpoB.setField("b", "hello");

    EventKey eventKeyB = new EventKey(1, 1, 1, gpoB);

    Assert.assertEquals("The two hashcodes should equal", eventKeyA.hashCode(), eventKeyB.hashCode());
    Assert.assertEquals("The two event keys should equal", eventKeyA, eventKeyB);
  }
}
