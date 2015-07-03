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
package com.datatorrent.lib.appdata.snapshot;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.schemas.DataQuerySnapshot;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestObjAllTypes;
import com.datatorrent.lib.util.TestUtils;

public class AppDataSnapshotServerPojoTest
{
  @Test
  public void simpleTest() throws Exception
  {
    String query = SchemaUtils.jarResourceFileToString("snapshotServerPojoQuery.json");
    String schema = SchemaUtils.jarResourceFileToString("snapshotServerPojoSchema.json");

    TestObjAllTypes objAllTypes = new TestObjAllTypes();

    Map<String, String> fieldToGetter = Maps.newHashMap();
    fieldToGetter.put("stringType", "getInnerObj().stringVal");
    fieldToGetter.put("charType", "getInnerObj().charVal");
    fieldToGetter.put("booleanType", "getInnerObj().boolVal");
    fieldToGetter.put("longType", "getInnerObj().longVal");
    fieldToGetter.put("integerType", "getInnerObj().intVal");
    fieldToGetter.put("shortType", "getInnerObj().shortVal");
    fieldToGetter.put("byteType", "getInnerObj().byteVal");
    fieldToGetter.put("floatType", "getInnerObj().floatVal");
    fieldToGetter.put("doubleType", "getInnerObj().doubleVal");

    AppDataSnapshotServerPOJO snapshotServer = new AppDataSnapshotServerPOJO();
    snapshotServer.setFieldToGetter(fieldToGetter);
    snapshotServer.setSnapshotSchemaJSON(schema);

    CollectorTestSink<String> resultSink = new CollectorTestSink<String>();
    TestUtils.setSink(snapshotServer.queryResult, resultSink);

    List<Object> inputList = Lists.newArrayList();
    inputList.add(objAllTypes);

    snapshotServer.setup(null);

    snapshotServer.beginWindow(0L);
    snapshotServer.input.put(inputList);
    snapshotServer.query.put(query);
    snapshotServer.endWindow();

    Assert.assertEquals(1, resultSink.collectedTuples.size());

    String result = resultSink.collectedTuples.get(0);

    JSONObject data = new JSONObject(result).getJSONArray(DataQuerySnapshot.FIELD_DATA).getJSONObject(0);

    Assert.assertEquals(objAllTypes.getInnerObj().stringVal, data.get("stringType"));
    Assert.assertEquals(Character.toString(objAllTypes.getInnerObj().charVal), data.get("charType"));
    Assert.assertEquals(Boolean.toString(objAllTypes.getInnerObj().boolVal), Boolean.toString((Boolean)data.get("booleanType")));
    Assert.assertEquals(Long.toString(objAllTypes.getInnerObj().longVal), data.get("longType"));
    Assert.assertEquals(Integer.toString(objAllTypes.getInnerObj().intVal), data.get("integerType"));
    Assert.assertEquals(Short.toString(objAllTypes.getInnerObj().shortVal), data.get("shortType"));
    Assert.assertEquals(Byte.toString(objAllTypes.getInnerObj().byteVal), data.get("byteType"));
    Assert.assertEquals(Float.toString(objAllTypes.getInnerObj().floatVal), data.get("floatType"));
    Assert.assertEquals(Double.toString(objAllTypes.getInnerObj().doubleVal), data.get("doubleType"));
  }

  private static final Logger LOG = LoggerFactory.getLogger(AppDataSnapshotServerPojoTest.class);
}
