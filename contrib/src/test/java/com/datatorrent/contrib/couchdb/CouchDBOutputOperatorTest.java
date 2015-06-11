/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.couchdb;

import java.net.MalformedURLException;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import java.util.ArrayList;

/**
 * Test for {@link MapBasedCouchDbOutputOperator}
 *
 * @since 0.3.5
 */
public class CouchDBOutputOperatorTest
{
  @Test
  public void testCouchDBOutputOperator() throws MalformedURLException
  {
    String testDocumentId = "TestDocument";
    Map<Object, Object> tuple = Maps.newHashMap();
    tuple.put("_id", testDocumentId);
    tuple.put("name", "TD");
    tuple.put("type", "test");

    MapBasedCouchDbOutputOperator dbOutputOper = new MapBasedCouchDbOutputOperator();
    CouchDbStore store = new CouchDbStore();
    store.setDbName(CouchDBTestHelper.TEST_DB);
    dbOutputOper.setStore(store);

    dbOutputOper.setup(new OperatorContextTestHelper.TestIdOperatorContext(1));
    dbOutputOper.beginWindow(0);
    dbOutputOper.input.process(tuple);
    dbOutputOper.endWindow();

    tuple.put("output-type", "map");

    dbOutputOper.beginWindow(1);
    dbOutputOper.input.process(tuple);
    dbOutputOper.endWindow();

    //Test if the document was persisted
    JsonNode docNode = CouchDBTestHelper.fetchDocument(testDocumentId);
    Assert.assertNotNull("Document saved", docNode);

    Assert.assertEquals("name of document", "TD", docNode.get("name").getTextValue());
    Assert.assertEquals("type of document", "test", docNode.get("type").getTextValue());
    Assert.assertEquals("output-type", "map", docNode.get("output-type").getTextValue());
  }

  @Test
  public void testCouchDBPOJOOutputOperator()
  {
    String testDocumentId = "test";
    TestPOJO tuple = new TestPOJO();
    tuple.setId(testDocumentId);
    tuple.setName("TD");
    tuple.setType("test");
    tuple.setRev("1-75efcce1f083316d622d389f3f9813f8");
    tuple.setOutput_type("pojo");
    CouchDbPOJOOutputOperator dbOutputOper = new CouchDbPOJOOutputOperator();
    CouchDbStore store = new CouchDbStore();
    store.setDbName(CouchDBTestHelper.TEST_DB);
    dbOutputOper.setStore(store);
    String expression = "getId()";
    ArrayList<String> expressions = new ArrayList<String>();
    expressions.add("rev");
    expressions.add("name");
    expressions.add("type");
    expressions.add("output_type");
    ArrayList<String> classNames = new ArrayList<String>();
    classNames.add("String");
    classNames.add("String");
    classNames.add("String");
    classNames.add("String");
    dbOutputOper.setClassNamesOfFields(classNames);
    dbOutputOper.setExpressions(expressions);
    dbOutputOper.setExpressionForDocId(expression);
    dbOutputOper.setup(new OperatorContextTestHelper.TestIdOperatorContext(1));
    dbOutputOper.beginWindow(0);
    dbOutputOper.input.process(tuple);
    dbOutputOper.endWindow();

    //Test if the document was persisted
    JsonNode docNode = CouchDBTestHelper.fetchDocument(testDocumentId);
    Assert.assertNotNull("Document saved", docNode);

    Assert.assertEquals("name of document", "TD", docNode.get("name").getTextValue());
    Assert.assertEquals("type of document", "test", docNode.get("type").getTextValue());
    Assert.assertEquals("output-type", "pojo", docNode.get("output_type").getTextValue());

  }

  public class TestPOJO
  {
    private String _id;
    private String output_type;
    private String rev;

    public String getRev()
    {
      return rev;
    }

    public void setRev(String rev)
    {
      this.rev = rev;
    }

    public String getOutput_type()
    {
      return output_type;
    }

    public void setOutput_type(String output_type)
    {
      this.output_type = output_type;
    }

    public String getId()
    {
      return _id;
    }

    public void setId(String id)
    {
      this._id = id;
    }

    public TestPOJO()
    {

    }

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public String getType()
    {
      return type;
    }

    public void setType(String type)
    {
      this.type = type;
    }

    String name;
    String type;

  }

  @BeforeClass
  public static void setup()
  {
    CouchDBTestHelper.setup();
  }

  @AfterClass
  public static void teardown()
  {
    CouchDBTestHelper.teardown();
  }
}
