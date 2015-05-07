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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class DataQueryTabularDeserializerTest
{
  @Test
  public void simpleDeserializerTest()
  {
    DataQueryTabularDeserializer deserializer = new DataQueryTabularDeserializer();

    String queryJSON = "{\n"
                       + "   \"id\": \"1\",\n"
                       + "   \"type\": \"dataQuery\",\n"
                       + "   \"data\": {\n"
                       + "      \"fields\": [ \"url\", \"count\" ]\n"
                       + "   }\n"
                       + "}";

    DataQueryTabular gQuery = (DataQueryTabular) deserializer.deserialize(queryJSON, null);

    Assert.assertEquals("The id must equal.", "1", gQuery.getId());
    Assert.assertEquals("The type must equal.", DataQueryTabular.TYPE, gQuery.getType());

    Fields fields = new Fields(Sets.newHashSet("url", "count"));

    Assert.assertEquals("The fields must equal.", fields, gQuery.getFields());
  }


  @Test
  public void simpleDeserializerWithSchemaKeysTest()
  {
    final Map<String, String> expectedSchemaKeys = Maps.newHashMap();
    expectedSchemaKeys.put("publisher", "google");
    expectedSchemaKeys.put("advertiser", "microsoft");
    expectedSchemaKeys.put("location", "CA");

    DataQueryTabularDeserializer deserializer = new DataQueryTabularDeserializer();

    String queryJSON = "{\n"
                       + "   \"id\": \"1\",\n"
                       + "   \"type\": \"dataQuery\",\n"
                       + "   \"data\": {\n"
                       + "      \"schemaKeys\":"
                       + "      {\"publisher\":\"google\",\"advertiser\":\"microsoft\",\"location\":\"CA\"},"
                       + "      \"fields\": [ \"url\", \"count\" ]\n"
                       + "   }\n"
                       + "}";

    DataQueryTabular gQuery = (DataQueryTabular) deserializer.deserialize(queryJSON, null);

    Assert.assertEquals("The id must equal.", "1", gQuery.getId());
    Assert.assertEquals("The type must equal.", DataQueryTabular.TYPE, gQuery.getType());

    Fields fields = new Fields(Sets.newHashSet("url", "count"));

    Assert.assertEquals("The fields must equal.", fields, gQuery.getFields());
    Assert.assertEquals(expectedSchemaKeys, gQuery.getSchemaKeys());
  }
}
