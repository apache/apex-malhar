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

import java.util.Map;

import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

public class SnapshotSchemaTest
{
  public static final String TEST_JSON = "{\n"
                                        + " \"values\": [\n"
                                        + "   {\"name\": \"url\", \"type\":\"string\"},\n"
                                        + "   {\"name\": \"count\", \"type\":\"integer\"}\n"
                                        + " ]\n"
                                        + "}";

  @Test
  public void schemaSnapshotFieldTypeTest()
  {
    SnapshotSchema schema = new SnapshotSchema(TEST_JSON);

    Assert.assertEquals("The schemaType must match.", SnapshotSchema.SCHEMA_TYPE, schema.getSchemaType());
    Assert.assertEquals("The schemaVersion must match", SnapshotSchema.SCHEMA_VERSION, schema.getSchemaVersion());

    Map<String, Type> expectedFieldToType = Maps.newHashMap();
    expectedFieldToType.put("url", Type.STRING);
    expectedFieldToType.put("count", Type.INTEGER);

    Map<String, Type> fieldToType = schema.getValuesDescriptor().getFieldToType();

    Assert.assertEquals("The field to type must match", expectedFieldToType, fieldToType);
  }
}
