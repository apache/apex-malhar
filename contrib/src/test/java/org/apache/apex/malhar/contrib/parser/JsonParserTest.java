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
package org.apache.apex.malhar.contrib.parser;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.appdata.schemas.SchemaUtils;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.fasterxml.jackson.annotation.JsonFormat;

public class JsonParserTest
{
  private static final String filename = "json-parser-schema.json";
  CollectorTestSink<Object> error = new CollectorTestSink<Object>();
  CollectorTestSink<Object> objectPort = new CollectorTestSink<Object>();
  CollectorTestSink<Object> pojoPort = new CollectorTestSink<Object>();
  JsonParser parser = new JsonParser();

  @Rule
  public Watcher watcher = new Watcher();

  public class Watcher extends TestWatcher
  {
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      parser.err.setSink(error);
      parser.parsedOutput.setSink(objectPort);
      parser.out.setSink(pojoPort);
      parser.setClazz(Product.class);
      parser.setJsonSchema(SchemaUtils.jarResourceFileToString(filename));
      parser.setup(null);
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      error.clear();
      objectPort.clear();
      pojoPort.clear();
      parser.teardown();
    }
  }

  @Test
  public void testValidInput() throws JSONException
  {
    String tuple = "{" + "\"id\": 2," + "\"name\": \"An ice sculpture\"," + "\"price\": 1,"
        + "\"tags\": [\"cold\", \"ice\"]," + "\"dimensions\": {" + "\"length\": 7.0," + "\"width\" : 8.0,"
        + "\"height\": 9.5" + "}," + "\"warehouseLocation\": {" + "\"latitude\": -78.75," + "\"longitude\": 20.4"
        + "}," + "\"dateOfManufacture\": \"2013/09/29\"," + "\"dateOfExpiry\": \"2013\"" + "}";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(Product.class, obj.getClass());
    Product pojo = (Product)obj;
    JSONObject jsonObject = (JSONObject)objectPort.collectedTuples.get(0);
    Assert.assertEquals(2, jsonObject.getInt("id"));
    Assert.assertEquals(1, jsonObject.getInt("price"));
    Assert.assertEquals("An ice sculpture", jsonObject.get("name"));
    Assert.assertEquals(7.0, jsonObject.getJSONObject("dimensions").getDouble("length"), 0);
    Assert.assertEquals(2, pojo.getId());
    Assert.assertEquals(1, pojo.getPrice());
    Assert.assertEquals("An ice sculpture", pojo.getName());
    Assert.assertEquals(7.0, (double)pojo.getDimensions().get("length"), 0);
  }

  @Test
  public void testEmptyInput() throws JSONException
  {
    parser.setSchema(null);
    String tuple = "";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void testValidInputWithoutSchema() throws JSONException
  {
    parser.setSchema(null);
    String tuple = "{" + "\"id\": 2," + "\"name\": \"An ice sculpture\"," + "\"price\": 1,"
        + "\"tags\": [\"cold\", \"ice\"]," + "\"dimensions\": {" + "\"length\": 7.0," + "\"width\" : 8.0,"
        + "\"height\": 9.5" + "}," + "\"warehouseLocation\": {" + "\"latitude\": -78.75," + "\"longitude\": 20.4"
        + "}," + "\"dateOfManufacture\": \"2013/09/29\"," + "\"dateOfExpiry\": \"2013\"" + "}";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(Product.class, obj.getClass());
    Product pojo = (Product)obj;
    JSONObject jsonObject = (JSONObject)objectPort.collectedTuples.get(0);
    Assert.assertEquals(2, jsonObject.getInt("id"));
    Assert.assertEquals(1, jsonObject.getInt("price"));
    Assert.assertEquals("An ice sculpture", jsonObject.get("name"));
    Assert.assertEquals(7.0, jsonObject.getJSONObject("dimensions").getDouble("length"), 0);
    Assert.assertEquals(2, pojo.getId());
    Assert.assertEquals(1, pojo.getPrice());
    Assert.assertEquals("An ice sculpture", pojo.getName());
    Assert.assertEquals(7.0, (double)pojo.getDimensions().get("length"), 0);
  }

  @Test
  public void testUnknowFieldsInData() throws JSONException
  {
    parser.setSchema(null);
    String tuple = "{" + "\"id\": 2," + "\"id2\": 2," + "\"name\": \"An ice sculpture\"," + "\"price\": 1,"
        + "\"tags\": [\"cold\", \"ice\"]," + "\"dimensions\": {" + "\"length\": 7.0," + "\"width\" : 8.0,"
        + "\"height\": 9.5" + "}" + "}";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(Product.class, obj.getClass());
    Product pojo = (Product)obj;
    JSONObject jsonObject = (JSONObject)objectPort.collectedTuples.get(0);
    Assert.assertEquals(2, jsonObject.getInt("id"));
    Assert.assertEquals(1, jsonObject.getInt("price"));
    Assert.assertEquals("An ice sculpture", jsonObject.get("name"));
    Assert.assertEquals(7.0, jsonObject.getJSONObject("dimensions").getDouble("length"), 0);
    Assert.assertEquals(2, pojo.getId());
    Assert.assertEquals(1, pojo.getPrice());
    Assert.assertEquals("An ice sculpture", pojo.getName());
    Assert.assertEquals(7.0, (double)pojo.getDimensions().get("length"), 0);
    Assert.assertNull(pojo.getWarehouseLocation());
  }

  @Test
  public void testInvalidPrice() throws JSONException
  {
    String tuple = "{" + "\"id\": 2," + "\"name\": \"An ice sculpture\"," + "\"price\": -1,"
        + "\"tags\": [\"cold\", \"ice\"]," + "\"dimensions\": {" + "\"length\": 7.0," + "\"width\" : 8.0,"
        + "\"height\": 9.5" + "}," + "\"warehouseLocation\": {" + "\"latitude\": -78.75," + "\"longitude\": 20.4"
        + "}," + "\"dateOfManufacture\": \"2013/09/29\"," + "\"dateOfExpiry\": \"2013\"" + "}";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String, String> errorKeyValPair = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(tuple, errorKeyValPair.getKey());
    Assert.assertEquals("\"/price\":\"number is lower than the required minimum\"", errorKeyValPair.getValue());
  }

  @Test
  public void testMultipleViolations() throws JSONException
  {
    String tuple = "{" + "\"id\": 2," + "\"name\": \"An ice sculpture\"," + "\"price\": -1,"
        + "\"tags\": [\"cold\", \"ice\"]," + "\"dimensions\": {" + "\"width\" : 8.0," + "\"height\": 9.5" + "},"
        + "\"warehouseLocation\": {" + "\"latitude\": -78.75," + "\"longitude\": 20.4" + "},"
        + "\"dateOfManufacture\": \"2013/09/29\"," + "\"dateOfExpiry\": \"2013\"" + "}";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String, String> errorKeyValPair = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(tuple, errorKeyValPair.getKey());
    Assert.assertEquals(
        "\"/dimensions\":\"missing required property(ies)\",\"/price\":\"number is lower than the required minimum\"",
        errorKeyValPair.getValue());
  }

  @Test
  public void testJsonSyntaxError() throws JSONException
  {
    String tuple = "{" + "\"id\": 2," + "\"name\": \"An ice sculpture\"," + "\"price\": -1,"
        + "\"tags\": [\"cold\", \"ice\"]," + "\"dimensions\": {" + "\"width\" : 8.0," + "\"height\": 9.5" + "}"
        + "\"warehouseLocation\": {" + "\"latitude\": -78.75," + "\"longitude\": 20.4" + "},"
        + "\"dateOfManufacture\": \"2013/09/29\"," + "\"dateOfExpiry\": \"2013\"" + "}";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String, String> errorKeyValPair = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(tuple, errorKeyValPair.getKey());
  }

  @Test
  public void testValidInputPojoPortNotConnected() throws JSONException
  {
    parser.out.setSink(null);
    String tuple = "{" + "\"id\": 2," + "\"name\": \"An ice sculpture\"," + "\"price\": 1,"
        + "\"tags\": [\"cold\", \"ice\"]," + "\"dimensions\": {" + "\"length\": 7.0," + "\"width\" : 8.0,"
        + "\"height\": 9.5" + "}," + "\"warehouseLocation\": {" + "\"latitude\": -78.75," + "\"longitude\": 20.4"
        + "}," + "\"dateOfManufacture\": \"2013/09/29\"," + "\"dateOfExpiry\": \"2013\"" + "}";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    JSONObject jsonObject = (JSONObject)objectPort.collectedTuples.get(0);
    Assert.assertEquals(2, jsonObject.getInt("id"));
    Assert.assertEquals(1, jsonObject.getInt("price"));
    Assert.assertEquals("An ice sculpture", jsonObject.get("name"));
    Assert.assertEquals(7.0, jsonObject.getJSONObject("dimensions").getDouble("length"), 0);
  }

  @Test
  public void testValidInputParsedOutputPortNotConnected() throws JSONException
  {
    parser.parsedOutput.setSink(null);
    String tuple = "{" + "\"id\": 2," + "\"name\": \"An ice sculpture\"," + "\"price\": 1,"
        + "\"tags\": [\"cold\", \"ice\"]," + "\"dimensions\": {" + "\"length\": 7.0," + "\"width\" : 8.0,"
        + "\"height\": 9.5" + "}," + "\"warehouseLocation\": {" + "\"latitude\": -78.75," + "\"longitude\": 20.4"
        + "}," + "\"dateOfManufacture\": \"2013/09/29\"," + "\"dateOfExpiry\": \"2013\"" + "}";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(Product.class, obj.getClass());
    Product pojo = (Product)obj;
    Assert.assertEquals(2, pojo.getId());
    Assert.assertEquals(1, pojo.getPrice());
    Assert.assertEquals("An ice sculpture", pojo.getName());
    Assert.assertEquals(7.0, (double)pojo.getDimensions().get("length"), 0);
  }

  @Test
  public void testParserValidInputMetricVerification()
  {
    parser.beginWindow(0);
    Assert.assertEquals(0, parser.parsedOutputCount);
    Assert.assertEquals(0, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(0, parser.getEmittedObjectCount());
    String tuple = "{" + "\"id\": 2," + "\"name\": \"An ice sculpture\"," + "\"price\": 1,"
        + "\"tags\": [\"cold\", \"ice\"]," + "\"dimensions\": {" + "\"length\": 7.0," + "\"width\" : 8.0,"
        + "\"height\": 9.5" + "}," + "\"warehouseLocation\": {" + "\"latitude\": -78.75," + "\"longitude\": 20.4"
        + "}," + "\"dateOfManufacture\": \"2013/09/29\"," + "\"dateOfExpiry\": \"2013\"" + "}";
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Assert.assertEquals(1, parser.parsedOutputCount);
    Assert.assertEquals(1, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(1, parser.getEmittedObjectCount());
  }

  @Test
  public void testParserInvalidInputMetricVerification()
  {
    parser.beginWindow(0);
    Assert.assertEquals(0, parser.parsedOutputCount);
    Assert.assertEquals(0, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(0, parser.getEmittedObjectCount());
    String tuple = "{" + "\"id\": 2" + "}";
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, parser.parsedOutputCount);
    Assert.assertEquals(1, parser.getIncomingTuplesCount());
    Assert.assertEquals(1, parser.getErrorTupleCount());
    Assert.assertEquals(0, parser.getEmittedObjectCount());
  }

  @Test
  public void testParserMetricResetVerification()
  {
    Assert.assertEquals(0, parser.parsedOutputCount);
    Assert.assertEquals(0, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(0, parser.getEmittedObjectCount());
    String tuple = "{" + "\"id\": 2," + "\"name\": \"An ice sculpture\"," + "\"price\": 1,"
        + "\"tags\": [\"cold\", \"ice\"]," + "\"dimensions\": {" + "\"length\": 7.0," + "\"width\" : 8.0,"
        + "\"height\": 9.5" + "}," + "\"warehouseLocation\": {" + "\"latitude\": -78.75," + "\"longitude\": 20.4"
        + "}," + "\"dateOfManufacture\": \"2013/09/29\"," + "\"dateOfExpiry\": \"2013\"" + "}";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Assert.assertEquals(1, parser.parsedOutputCount);
    Assert.assertEquals(1, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(1, parser.getEmittedObjectCount());
    parser.beginWindow(1);
    Assert.assertEquals(0, parser.parsedOutputCount);
    Assert.assertEquals(0, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(0, parser.getEmittedObjectCount());
    parser.in.process(tuple.getBytes());
    Assert.assertEquals(1, parser.parsedOutputCount);
    Assert.assertEquals(1, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(1, parser.getEmittedObjectCount());
    parser.endWindow();
  }

  public static class Product
  {
    public int id;
    public int price;
    public String name;
    public List<String> tags;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy/MM/dd")
    public Date dateOfManufacture;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy")
    public Date dateOfExpiry;
    public Map<String, Object> dimensions;
    public Map<String, Object> warehouseLocation;

    public int getId()
    {
      return id;
    }

    public void setId(int id)
    {
      this.id = id;
    }

    public int getPrice()
    {
      return price;
    }

    public void setPrice(int price)
    {
      this.price = price;
    }

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public List<String> getTags()
    {
      return tags;
    }

    public void setTags(List<String> tags)
    {
      this.tags = tags;
    }

    public Date getDateOfManufacture()
    {
      return dateOfManufacture;
    }

    public void setDateOfManufacture(Date dateOfManufacture)
    {
      this.dateOfManufacture = dateOfManufacture;
    }

    public Map<String, Object> getDimensions()
    {
      return dimensions;
    }

    public void setDimensions(Map<String, Object> dimensions)
    {
      this.dimensions = dimensions;
    }

    public Map<String, Object> getWarehouseLocation()
    {
      return warehouseLocation;
    }

    public void setWarehouseLocation(Map<String, Object> warehouseLocation)
    {
      this.warehouseLocation = warehouseLocation;
    }

    public Date getDateOfExpiry()
    {
      return dateOfExpiry;
    }

    public void setDateOfExpiry(Date dateOfExpiry)
    {
      this.dateOfExpiry = dateOfExpiry;
    }

    @Override
    public String toString()
    {
      return "Product [id=" + id + ", price=" + price + ", name=" + name + ", tags=" + tags + ", dateOfManufacture="
          + dateOfManufacture + ", dateOfExpiry=" + dateOfExpiry + ", dimensions=" + dimensions
          + ", warehouseLocation=" + warehouseLocation + "]";
    }
  }
}
