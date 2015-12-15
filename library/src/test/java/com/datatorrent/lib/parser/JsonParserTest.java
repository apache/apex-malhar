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
package com.datatorrent.lib.parser;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperatorTest.FSTestWatcher;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.lib.util.TestUtils.TestInfo;

public class JsonParserTest
{
  JsonParser operator;
  CollectorTestSink<Object> validDataSink;
  CollectorTestSink<String> invalidDataSink;

  final ByteArrayOutputStream myOut = new ByteArrayOutputStream();

  public JsonParserTest()
  {
    // So that the output is cleaner.
    System.setErr(new PrintStream(myOut));
  }

  @Rule
  public TestInfo testMeta = new FSTestWatcher()
  {
    private void deleteDirectory()
    {
      try {
        FileUtils.deleteDirectory(new File(getDir()));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    protected void starting(Description descriptor)
    {

      super.starting(descriptor);
      deleteDirectory();

      operator = new JsonParser();
      operator.setClazz(Test1Pojo.class);
      validDataSink = new CollectorTestSink<Object>();
      invalidDataSink = new CollectorTestSink<String>();
      TestUtils.setSink(operator.out, validDataSink);
      TestUtils.setSink(operator.err, invalidDataSink);
      operator.setup(null);
      operator.activate(null);

      operator.beginWindow(0);
    }

    @Override
    protected void finished(Description description)
    {
      operator.endWindow();
      operator.teardown();

      deleteDirectory();
      super.finished(description);
    }
  };

  @Test
  public void testJSONToPOJO()
  {
    String tuple = "{\"a\":123,\"b\":234876274,\"c\":\"HowAreYou?\",\"d\":[\"ABC\",\"PQR\",\"XYZ\"]}";
    operator.in.put(tuple);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    Object obj = validDataSink.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(Test1Pojo.class, obj.getClass());
    Test1Pojo pojo = (Test1Pojo)obj;
    Assert.assertEquals(123, pojo.a);
    Assert.assertEquals(234876274, pojo.b);
    Assert.assertEquals("HowAreYou?", pojo.c);
    Assert.assertEquals(3, pojo.d.size());
    Assert.assertEquals("ABC", pojo.d.get(0));
    Assert.assertEquals("PQR", pojo.d.get(1));
    Assert.assertEquals("XYZ", pojo.d.get(2));
  }

  @Test
  public void testJSONToPOJODate()
  {
    String tuple = "{\"a\":123,\"b\":234876274,\"c\":\"HowAreYou?\",\"d\":[\"ABC\",\"PQR\",\"XYZ\"],\"date\":\"15-09-2015\"}";
    operator.setDateFormat("dd-MM-yyyy");
    operator.setup(null);
    operator.activate(null);
    operator.in.put(tuple);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    Object obj = validDataSink.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(Test1Pojo.class, obj.getClass());
    Test1Pojo pojo = (Test1Pojo)obj;
    Assert.assertEquals(123, pojo.a);
    Assert.assertEquals(234876274, pojo.b);
    Assert.assertEquals("HowAreYou?", pojo.c);
    Assert.assertEquals(3, pojo.d.size());
    Assert.assertEquals("ABC", pojo.d.get(0));
    Assert.assertEquals("PQR", pojo.d.get(1));
    Assert.assertEquals("XYZ", pojo.d.get(2));
    Assert.assertEquals(2015, new DateTime(pojo.date).getYear());
    Assert.assertEquals(9, new DateTime(pojo.date).getMonthOfYear());
    Assert.assertEquals(15, new DateTime(pojo.date).getDayOfMonth());
  }

  @Test
  public void testJSONToPOJOInvalidData()
  {
    String tuple = "{\"a\":123\"b\":234876274,\"c\":\"HowAreYou?\"}";
    operator.in.put(tuple);
    Assert.assertEquals(0, validDataSink.collectedTuples.size());
    Assert.assertEquals(1, invalidDataSink.collectedTuples.size());
    Assert.assertEquals(tuple, invalidDataSink.collectedTuples.get(0));
  }

  @Test
  public void testJSONToPOJOUnknownFields()
  {
    String tuple = "{\"a\":123,\"b\":234876274,\"c\":\"HowAreYou?\",\"asd\":433.6}";
    operator.in.put(tuple);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    Object obj = validDataSink.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(Test1Pojo.class, obj.getClass());
    Test1Pojo pojo = (Test1Pojo)obj;
    Assert.assertEquals(123, pojo.a);
    Assert.assertEquals(234876274, pojo.b);
    Assert.assertEquals("HowAreYou?", pojo.c);
    Assert.assertEquals(null, pojo.d);
  }

  @Test
  public void testJSONToPOJOMismatchingFields()
  {
    String tuple = "{\"a\":123,\"c\":234876274,\"b\":\"HowAreYou?\",\"d\":[\"ABC\",\"PQR\",\"XYZ\"]}";
    operator.in.put(tuple);
    Assert.assertEquals(0, validDataSink.collectedTuples.size());
    Assert.assertEquals(1, invalidDataSink.collectedTuples.size());
    Assert.assertEquals(tuple, invalidDataSink.collectedTuples.get(0));
  }

  @Test
  public void testJSONToPOJOEmptyString()
  {
    String tuple = "";
    operator.in.put(tuple);
    Assert.assertEquals(0, validDataSink.collectedTuples.size());
    Assert.assertEquals(1, invalidDataSink.collectedTuples.size());
    Assert.assertEquals(tuple, invalidDataSink.collectedTuples.get(0));
  }

  @Test
  public void testJSONToPOJOEmptyJSON()
  {
    String tuple = "{}";
    operator.in.put(tuple);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    Object obj = validDataSink.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(Test1Pojo.class, obj.getClass());
    Test1Pojo pojo = (Test1Pojo)obj;
    Assert.assertEquals(0, pojo.a);
    Assert.assertEquals(0, pojo.b);
    Assert.assertEquals(null, pojo.c);
    Assert.assertEquals(null, pojo.d);
  }

  @Test
  public void testJSONToPOJOArrayInJson()
  {
    String tuple = "{\"a\":123,\"c\":[234,65,23],\"b\":\"HowAreYou?\",\"d\":[\"ABC\",\"PQR\",\"XYZ\"]}";
    operator.in.put(tuple);
    Assert.assertEquals(0, validDataSink.collectedTuples.size());
    Assert.assertEquals(1, invalidDataSink.collectedTuples.size());
    Assert.assertEquals(tuple, invalidDataSink.collectedTuples.get(0));
  }

  public static class Test1Pojo
  {
    public int a;
    public long b;
    public String c;
    public List<String> d;
    public Date date;

    @Override
    public String toString()
    {
      return "Test1Pojo [a=" + a + ", b=" + b + ", c=" + c + ", d=" + d + ", date=" + date + "]";
    }
  }
}
