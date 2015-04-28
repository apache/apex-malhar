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

package com.datatorrent.lib.appdata.tabular;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.SchemaTabular;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TabularMapConverterTest
{
  @Test
  public void simpleTest()
  {
    final String boolField = "boolField";
    final String intField = "intField";
    final String doubleField = "doubleField";

    final String mapBoolField = "mapBoolField";
    final String mapIntField = "mapIntField";

    final boolean expectedBool = true;
    final int expectedInt = 3;
    final double expectedDouble = 2.5;

    String tabularSchemaJSON = SchemaUtils.jarResourceFileToString("tabularschema.json");
    SchemaTabular tabularSchema = new SchemaTabular(tabularSchemaJSON);

    final Map<String, String> fieldToMapField = Maps.newHashMap();
    fieldToMapField.put(boolField, mapBoolField);
    fieldToMapField.put(intField, mapIntField);

    TabularMapConverter mapConverter = new TabularMapConverter();

    mapConverter.setTableFieldToMapField(fieldToMapField);

    Map<String, Object> inputMap = Maps.newHashMap();
    inputMap.put(mapBoolField, expectedBool);
    inputMap.put(mapIntField, expectedInt);
    inputMap.put(doubleField, expectedDouble);

    GPOMutable converted = mapConverter.convert(inputMap, tabularSchema);

    Assert.assertEquals(expectedBool, converted.getFieldBool(boolField));
    Assert.assertEquals(expectedInt, converted.getFieldInt(intField));
    Assert.assertEquals(expectedDouble, converted.getFieldDouble(doubleField), 0.0);
  }
}
