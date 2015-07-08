/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.Maps;

import org.codehaus.jackson.JsonNode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

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
    String testDocumentId = "test2";
    TestPOJO tuple = new TestPOJO();
    tuple.setId(testDocumentId);
    tuple.setName("TD2");
    tuple.setType("test2");
    tuple.setCh('x');
    tuple.setFl(2.0f);
    tuple.setOutput_type("pojo");
    Address address = new Address();
    address.setCity("chandigarh");
    address.setHousenumber(123);
    tuple.setAddress(address);
    CouchDBPOJOOutputOperator dbOutputOper = new CouchDBPOJOOutputOperator();
    CouchDbStore store = new CouchDbStore();
    store.setDbName(CouchDBTestHelper.TEST_DB);
    dbOutputOper.setStore(store);
    String expression = "getId()";
    dbOutputOper.setExpressionForDocId(expression);
    dbOutputOper.setup(new OperatorContextTestHelper.TestIdOperatorContext(1));
    dbOutputOper.beginWindow(0);
    dbOutputOper.input.process(tuple);
    dbOutputOper.endWindow();

    //Test if the document was persisted
    JsonNode docNode = CouchDBTestHelper.fetchDocument(testDocumentId);
    Assert.assertNotNull("Document saved ", docNode);
    Assert.assertEquals("name of document ", "TD2", docNode.get("name").getTextValue());
    Assert.assertEquals("type of document ", "test2", docNode.get("type").getTextValue());
    Assert.assertEquals("character ", "x", docNode.get("ch").getTextValue());
    Assert.assertEquals("float value ", 2.0, docNode.get("fl").getDoubleValue());

    Assert.assertEquals("output-type", "pojo", docNode.get("output_type").getTextValue());
    Assert.assertEquals("Housenumber is ",123,docNode.get("address").get("housenumber").getIntValue());
    Assert.assertEquals("City is ","chandigarh",docNode.get("address").get("city").getTextValue());

  }
  public class TestPOJO
  {
    private String _id;
    private String output_type;
    private String revision;
    private Address address;
    private  String name;
    private String type;
    private char ch;
    private float fl;

    public float getFl()
    {
      return fl;
    }

    public void setFl(float fl)
    {
      this.fl = fl;
    }

    public char getCh()
    {
      return ch;
    }

    public void setCh(char ch)
    {
      this.ch = ch;
    }

    public TestPOJO()
    {

    }

    public Address getAddress()
    {
      return address;
    }

    public void setAddress(Address address)
    {
      this.address = address;
    }

    public String getRevision()
    {
      return revision;
    }

    public void setRevision(String rev)
    {
      this.revision = rev;
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

  }

  private class Address
  {
    private int housenumber;

    public int getHousenumber()
    {
      return housenumber;
    }

    public void setHousenumber(int housenumber)
    {
      this.housenumber = housenumber;
    }

    public String getCity()
    {
      return city;
    }

    public void setCity(String city)
    {
      this.city = city;
    }
    private String city;



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
