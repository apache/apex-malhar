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

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class DataResultTabularSerializerTest
{
  private static final Logger logger = LoggerFactory.getLogger(DataResultTabularSerializerTest.class);

  @Test
  public void simpleResultSerializerTest()
  {
    Map<String, Type> fieldToTypeA = Maps.newHashMap();
    fieldToTypeA.put("a", Type.LONG);
    fieldToTypeA.put("b", Type.STRING);

    FieldsDescriptor fdA = new FieldsDescriptor(fieldToTypeA);

    GPOMutable gpoA = new GPOMutable(fdA);
    gpoA.setField("a", 1L);
    gpoA.setField("b", "hello");

    FieldsDescriptor fdB = new FieldsDescriptor(fieldToTypeA);

    GPOMutable gpoB = new GPOMutable(fdB);
    gpoB.setField("a", 2L);
    gpoB.setField("b", "world");

    List<GPOMutable> values = Lists.newArrayList();
    values.add(gpoA);
    values.add(gpoB);

    DataQueryTabular gQuery = new DataQueryTabular("1",
                                                                 DataQueryTabular.TYPE,
                                                                 new Fields(Sets.newHashSet("a", "b")));

    DataResultTabular result = new DataResultTabular(gQuery,
                                                                   values);
    result.setType(DataResultTabular.TYPE);

    DataResultTabularSerializer serializer = new DataResultTabularSerializer();

    final String expectedJSONResult =
    "{\"id\":\"1\",\"type\":\"dataResult\",\"data\":[{\"b\":\"hello\",\"a\":1},{\"b\":\"world\",\"a\":2}]}";

    Assert.assertEquals("The json doesn't match.", expectedJSONResult, serializer.serialize(result, null));
  }
}
