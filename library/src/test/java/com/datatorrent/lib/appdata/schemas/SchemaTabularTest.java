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
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SchemaTabularTest
{
  @Test
  public void schemaTabularTest()
  {
    String schemaJSON = "{\n"
                        + " \"schemaType\": \"twitterTop10\",\n"
                        + " \"schemaVersion\": \"1.0\",\n"
                        + " \"values\": [\n"
                        + "   {\"name\": \"url\", \"type\":\"string\"},\n"
                        + "   {\"name\": \"count\", \"type\":\"integer\"}\n"
                        + " ]\n"
                        + "}";
    SchemaTabular schema = new SchemaTabular(schemaJSON);

    Assert.assertEquals("The schemaType must match.", "twitterTop10", schema.getSchemaType());
    Assert.assertEquals("The schemaVersion must match", "1.0", schema.getSchemaVersion());

    Map<String, Type> expectedFieldToType = Maps.newHashMap();
    expectedFieldToType.put("url", Type.STRING);
    expectedFieldToType.put("count", Type.INTEGER);

    Map<String, Type> fieldToType = schema.getValuesDescriptor().getFieldToType();

    Assert.assertEquals("The field to type must match", expectedFieldToType, fieldToType);
  }
}
