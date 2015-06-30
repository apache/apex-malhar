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
package com.datatorrent.lib.appdata.schemas;

import java.util.Collections;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.util.TestUtils;

public class SchemaRegistryMultipleTest
{
  private static final Map<String, String> SCHEMA_ADS_KEYS;
  private static final Map<String, String> SCHEMA_SALES_KEYS;

  static {
    Map<String, String> schemaAdsKeys = Maps.newHashMap();
    schemaAdsKeys.put("type", "ads");
    Map<String, String> schemaSalesKeys = Maps.newHashMap();
    schemaSalesKeys.put("type", "sales");

    SCHEMA_ADS_KEYS = Collections.unmodifiableMap(schemaAdsKeys);
    SCHEMA_SALES_KEYS = Collections.unmodifiableMap(schemaSalesKeys);
  }

  @Test
  public void serializationTest() throws Exception
  {
    SchemaRegistryMultiple schemaRegistryMultiple = createSchemaRegistry();

    schemaRegistryMultiple = TestUtils.clone(new Kryo(), schemaRegistryMultiple);

    Assert.assertEquals(2, schemaRegistryMultiple.size());
  }

  @Test
  public void simpleTest()
  {
    final String id = "1";

    SchemaRegistryMultiple registry = createSchemaRegistry();

    //Get schema for ads
    Schema tempAdsSchema = registry.getSchema(SCHEMA_ADS_KEYS);
    Assert.assertEquals(SCHEMA_ADS_KEYS, tempAdsSchema.getSchemaKeys());

    //Get schema for sales
    Schema tempSalesSchema = registry.getSchema(SCHEMA_SALES_KEYS);
    Assert.assertEquals(SCHEMA_SALES_KEYS, tempSalesSchema.getSchemaKeys());

    //Query schema for ads
    SchemaQuery schemaQueryAds = new SchemaQuery(id,
                                                 SCHEMA_ADS_KEYS);
    SchemaResult result = registry.getSchemaResult(schemaQueryAds);
    Assert.assertEquals(1, result.getGenericSchemas().length);
    Assert.assertEquals(SCHEMA_ADS_KEYS, result.getGenericSchemas()[0].getSchemaKeys());

    //Query schema for sales
    SchemaQuery schemaQuerySales = new SchemaQuery(id,
                                                   SCHEMA_SALES_KEYS);
    result = registry.getSchemaResult(schemaQuerySales);
    Assert.assertEquals(1, result.getGenericSchemas().length);
    Assert.assertEquals(SCHEMA_SALES_KEYS, result.getGenericSchemas()[0].getSchemaKeys());
  }

  private SchemaRegistryMultiple createSchemaRegistry()
  {
    MockSchema schemaAds = new MockSchema();
    final Map<String, String> schemaAdsKeys = Maps.newHashMap();
    schemaAdsKeys.put("type", "ads");

    MockSchema schemaSales = new MockSchema();
    final Map<String, String> schemaSalesKeys = Maps.newHashMap();
    schemaSalesKeys.put("type", "sales");

    SchemaRegistryMultiple registry = new SchemaRegistryMultiple(Lists.newArrayList("type"));
    registry.registerSchema(schemaAds, schemaAdsKeys);
    registry.registerSchema(schemaSales, schemaSalesKeys);

    return registry;
  }

  public static class MockSchema implements Schema
  {
    private Map<String, String> schemaKeys;

    public MockSchema()
    {
    }

    @Override
    public int getSchemaID()
    {
      return 0;
    }

    @Override
    public String getSchemaType()
    {
      return "mock";
    }

    @Override
    public String getSchemaVersion()
    {
      return "0";
    }

    @Override
    public String getSchemaJSON()
    {
      return "{\"hello\":\"world\"}";
    }

    @Override
    public Map<String, String> getSchemaKeys()
    {
      return schemaKeys;
    }

    @Override
    public void setSchemaKeys(Map<String, String> schemaKeys)
    {
      this.schemaKeys = schemaKeys;
    }
  }
}
