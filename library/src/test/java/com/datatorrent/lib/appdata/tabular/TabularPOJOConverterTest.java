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

import com.datatorrent.lib.appbuilder.convert.pojo.PojoFieldRetrieverExpression;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.SchemaTabular;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TabularPOJOConverterTest
{
  @Test
  public void simpleTest()
  {
    String tabularSchemaJSON = SchemaUtils.jarResourceFileToString("tabularschema.json");
    SchemaTabular tabularSchema = new SchemaTabular(tabularSchemaJSON);

    PojoFieldRetrieverExpression pfre = new PojoFieldRetrieverExpression();
    pfre.setFieldToType(tabularSchema.getFieldToType());

    Map<String, String> fieldToExpression = Maps.newHashMap();
    fieldToExpression.put("boolField", "boolField");
    fieldToExpression.put("intField", "intField");
    fieldToExpression.put("doubleField", "doubleField");

    pfre.setFieldToExpression(fieldToExpression);
    pfre.setFQClassName(SimplePOJO.class.getName());

    TabularPOJOConverter converter = new TabularPOJOConverter();
    converter.setPojoFieldRetriever(pfre);

    SimplePOJO testPOJO = new SimplePOJO();
    GPOMutable result = converter.convert(testPOJO, tabularSchema);

    Assert.assertEquals(testPOJO.boolField, result.getField("boolField"));
    Assert.assertEquals(testPOJO.intField, result.getField("intField"));
    Assert.assertEquals(testPOJO.doubleField, result.getField("doubleField"));
  }

  public class SimplePOJO
  {
    public boolean boolField = true;
    public int intField = 3;
    public double doubleField = 5.5;
  }
}
