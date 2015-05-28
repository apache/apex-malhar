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

import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.dimensions.aggregator.AggregatorUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

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

  @Before
  public void initialize()
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
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
    final String id = "1";
    String adsJSON = SchemaUtils.jarResourceFileToString("adsGenericEventSchema.json");
    String salesJSON = SchemaUtils.jarResourceFileToString("salesGenericEventSchema.json");

    DimensionalSchema schemaAds = new DimensionalSchema(new DimensionalConfigurationSchema(adsJSON,
                                                                                   AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));
    final Map<String, String> schemaAdsKeys = Maps.newHashMap();
    schemaAdsKeys.put("type", "ads");

    DimensionalSchema schemaSales = new DimensionalSchema(new DimensionalConfigurationSchema(salesJSON,
                                                                                     AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));
    final Map<String, String> schemaSalesKeys = Maps.newHashMap();
    schemaSalesKeys.put("type", "sales");

    SchemaRegistryMultiple registry = new SchemaRegistryMultiple(Lists.newArrayList("type"));
    registry.registerSchema(schemaAds, schemaAdsKeys);
    registry.registerSchema(schemaSales, schemaSalesKeys);

    return registry;
  }
}
