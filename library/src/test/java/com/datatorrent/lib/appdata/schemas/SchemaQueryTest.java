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

import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SchemaQueryTest
{
  @Test
  public void jsonToSchemaQueryTest() throws Exception
  {
    final String id = "12345";
    final String schemaQueryJSON = "{" +
                                      "\"id\":\"" + id + "\"," +
                                      "\"type\":\"" + SchemaQuery.TYPE + "\"" +
                                    "}";



    @SuppressWarnings("unchecked")
    DataDeserializerFactory qb = new DataDeserializerFactory(SchemaQuery.class);

    SchemaQuery schemaQuery = (SchemaQuery) qb.deserialize(schemaQueryJSON);

    Assert.assertEquals("Id's must match", id, schemaQuery.getId());
    Assert.assertEquals("Types must match", SchemaQuery.TYPE, schemaQuery.getType());
  }

  @Test
  public void jsonToSchemaQueryWithSchemaKeysTest() throws Exception
  {
    final Map<String, String> expectedSchemaKeys = Maps.newHashMap();
    expectedSchemaKeys.put("publisher", "google");
    expectedSchemaKeys.put("advertiser", "microsoft");
    expectedSchemaKeys.put("location", "CA");

    final String id = "12345";
    final String schemaQueryJSON = "{" +
                                      "\"id\":\"" + id + "\"," +
                                      "\"type\":\"" + SchemaQuery.TYPE + "\"," +
                                      "\"schemaKeys\":" +
                                      "{\"publisher\":\"google\",\"advertiser\":\"microsoft\",\"location\":\"CA\"}" +
                                   "}";

    @SuppressWarnings("unchecked")
    DataDeserializerFactory qb = new DataDeserializerFactory(SchemaQuery.class);

    SchemaQuery schemaQuery = (SchemaQuery) qb.deserialize(schemaQueryJSON);

    Assert.assertEquals("Id's must match", id, schemaQuery.getId());
    Assert.assertEquals("Types must match", SchemaQuery.TYPE, schemaQuery.getType());
    Assert.assertEquals("Schema keys must match", expectedSchemaKeys, schemaQuery.getSchemaKeys());
  }
}
