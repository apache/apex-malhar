/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.appdata.schemas;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;

import org.apache.apex.malhar.lib.appdata.query.serde.DataQuerySnapshotDeserializer;
import org.apache.apex.malhar.lib.appdata.query.serde.MessageDeserializerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DataQuerySnapshotDeserializerTest
{
  @Rule
  public DataQuerySnapshotInfo testMeta = new DataQuerySnapshotInfo();

  public static class DataQuerySnapshotInfo extends TestWatcher
  {
    public MessageDeserializerFactory queryDeserializerFactory;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String snapshotSchemaJSON = SchemaUtils.jarResourceFileToString("snapshotschema.json");
      SchemaRegistrySingle schemaRegistry = new SchemaRegistrySingle(new SnapshotSchema(snapshotSchemaJSON));
      queryDeserializerFactory  = new MessageDeserializerFactory(SchemaQuery.class,
                                                                 DataQuerySnapshot.class);
      queryDeserializerFactory.setContext(DataQuerySnapshot.class, schemaRegistry);
    }
  }

  @Test
  public void simpleDeserializerTest() throws Exception
  {
    DataQuerySnapshotDeserializer deserializer = new DataQuerySnapshotDeserializer();

    String queryJSON = SchemaUtils.jarResourceFileToString("snapshotquery_deserialize1.json");

    DataQuerySnapshot gQuery = (DataQuerySnapshot)deserializer.deserialize(queryJSON, DataQuerySnapshot.class, null);

    Assert.assertEquals("The id must equal.", "1", gQuery.getId());
    Assert.assertEquals("The type must equal.", DataQuerySnapshot.TYPE, gQuery.getType());

    Fields fields = new Fields(Sets.newHashSet("url", "count"));

    Assert.assertEquals("The fields must equal.", fields, gQuery.getFields());
  }

  @Test
  public void simpleDeserializerWithSchemaKeysTest() throws Exception
  {
    final Map<String, String> expectedSchemaKeys = Maps.newHashMap();
    expectedSchemaKeys.put("publisher", "google");
    expectedSchemaKeys.put("advertiser", "microsoft");
    expectedSchemaKeys.put("location", "CA");

    DataQuerySnapshotDeserializer deserializer = new DataQuerySnapshotDeserializer();

    String queryJSON = SchemaUtils.jarResourceFileToString("snapshotquery_deserialize2.json");

    DataQuerySnapshot gQuery = (DataQuerySnapshot)deserializer.deserialize(queryJSON, DataQuerySnapshot.class, null);

    Assert.assertEquals("The id must equal.", "1", gQuery.getId());
    Assert.assertEquals("The type must equal.", DataQuerySnapshot.TYPE, gQuery.getType());

    Fields fields = new Fields(Sets.newHashSet("url", "count"));

    Assert.assertEquals("The fields must equal.", fields, gQuery.getFields());
    Assert.assertEquals(expectedSchemaKeys, gQuery.getSchemaKeys());
  }

  @Test
  public void noFieldsSpecified() throws Exception
  {
    String snapshotQuery = SchemaUtils.jarResourceFileToString("snapshotquery_deserialize3.json");
    DataQuerySnapshot query = (DataQuerySnapshot)testMeta.queryDeserializerFactory.deserialize(snapshotQuery);

    Set<String> expectedFields = Sets.newHashSet("boolField", "intField", "doubleField");

    Assert.assertEquals(expectedFields, query.getFields().getFields());
  }

  @Test
  public void validDeserialize1Test() throws Exception
  {
    testValid("snapshotquery_deserialize4.json");
  }

  @Test
  public void validDeserialize2Test() throws Exception
  {
    testValid("snapshotquery_deserialize5.json");
  }

  @Test
  public void validDeserialize3Test() throws Exception
  {
    testValid("snapshotquery_deserialize6.json");
  }

  @Test
  public void validDeserializeExtraFieldTest() throws Exception
  {
    testValid("snapshotquery_deserialize7.json");
  }

  @Test
  public void invalidTestCountdownValue() throws Exception
  {
    testInvalid("snapshotquery_invalidcountdown.json");
  }

  @Test
  public void invalidTest1() throws Exception
  {
    testInvalid("snapshotquery_validation1.json");
  }

  @Test
  public void invalidTest2() throws Exception
  {
    testInvalid("snapshotquery_validation2.json");
  }

  @Test
  public void invalidTest3() throws Exception
  {
    testInvalid("snapshotquery_validation3.json");
  }

  private void testInvalid(String invalidResourceJSON) throws Exception
  {
    boolean caughtException = false;

    try {
      String snapshotQuery = SchemaUtils.jarResourceFileToString(invalidResourceJSON);
      testMeta.queryDeserializerFactory.deserialize(snapshotQuery);
    } catch (IOException e) {
      caughtException = true;
    }

    Assert.assertTrue(caughtException);
  }

  private void testValid(String validResourceJSON) throws Exception
  {
    String snapshotQuery = SchemaUtils.jarResourceFileToString(validResourceJSON);
    DataQuerySnapshot query = (DataQuerySnapshot)testMeta.queryDeserializerFactory.deserialize(snapshotQuery);
    Assert.assertNotNull(query);
  }
}
