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

import com.datatorrent.lib.dimensions.AggregatorUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SchemaRegistrySingleTest
{
  @Before
  public void initialize()
  {
    AggregatorUtils.DEFAULT_AGGREGATOR_REGISTRY.setup();
  }

  @Test
  public void simpleTest()
  {
    final String id = "1";
    String eventSchemaJSON = SchemaUtils.jarResourceFileToString("adsGenericEventSchema.json");
    DimensionalSchema schemaDimensional = new DimensionalSchema(new DimensionalConfigurationSchema(eventSchemaJSON,
                                                                                           AggregatorUtils.DEFAULT_AGGREGATOR_REGISTRY));
    SchemaRegistrySingle schemaRegistrySingle = new SchemaRegistrySingle();
    schemaRegistrySingle.registerSchema(schemaDimensional);

    SchemaQuery schemaQuery = new SchemaQuery(id);
    SchemaResult schemaResult = schemaRegistrySingle.getSchemaResult(schemaQuery);

    Assert.assertEquals(1, schemaResult.getGenericSchemas().length);
    Assert.assertEquals(id, schemaResult.getId());
    Assert.assertTrue(schemaResult.getGenericSchemas()[0] != null);
  }
}
