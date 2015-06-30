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

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.appdata.schemas.SchemaRegistryMultipleTest.MockSchema;

public class SchemaRegistrySingleTest
{
  @Test
  public void simpleTest()
  {
    final String id = "1";

    MockSchema schemaDimensional = new MockSchema();
    SchemaRegistrySingle schemaRegistrySingle = new SchemaRegistrySingle();
    schemaRegistrySingle.registerSchema(schemaDimensional);

    SchemaQuery schemaQuery = new SchemaQuery(id);
    SchemaResult schemaResult = schemaRegistrySingle.getSchemaResult(schemaQuery);

    Assert.assertEquals(1, schemaResult.getGenericSchemas().length);
    Assert.assertEquals(id, schemaResult.getId());
    Assert.assertTrue(schemaResult.getGenericSchemas()[0] != null);
  }
}
