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
package com.datatorrent.contrib.schema.formatter;

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
import com.google.common.collect.Lists;

public class JsonFormatterTest
{
  JsonFormatter operator;
  CollectorTestSink<Object> validDataSink;
  CollectorTestSink<String> invalidDataSink;

  final ByteArrayOutputStream myOut = new ByteArrayOutputStream();

  public JsonFormatterTest()
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

      operator = new JsonFormatter();

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
    Test1Pojo pojo = new Test1Pojo();
    pojo.a = 123;
    pojo.b = 234876274;
    pojo.c = "HowAreYou?";
    pojo.d = Lists.newArrayList("ABC", "PQR", "XYZ");

    operator.in.put(pojo);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expectedJSONString = "{\"a\":123,\"b\":234876274,\"c\":\"HowAreYou?\",\"d\":[\"ABC\",\"PQR\",\"XYZ\"],\"date\":null}";
    Assert.assertEquals(expectedJSONString, validDataSink.collectedTuples.get(0));
  }

  @Test
  public void testJSONToPOJODate()
  {
    Test1Pojo pojo = new Test1Pojo();
    pojo.a = 123;
    pojo.b = 234876274;
    pojo.c = "HowAreYou?";
    pojo.d = Lists.newArrayList("ABC", "PQR", "XYZ");
    pojo.date = new DateTime().withYear(2015).withMonthOfYear(9).withDayOfMonth(15).toDate();
    operator.setDateFormat("dd-MM-yyyy");
    operator.setup(null);
    operator.activate(null);
    operator.in.put(pojo);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expectedJSONString = "{\"a\":123,\"b\":234876274,\"c\":\"HowAreYou?\",\"d\":[\"ABC\",\"PQR\",\"XYZ\"],\"date\":\"15-09-2015\"}";
    Assert.assertEquals(expectedJSONString, validDataSink.collectedTuples.get(0));
  }

  @Test
  public void testJSONToPOJONullFields()
  {
    Test1Pojo pojo = new Test1Pojo();
    pojo.a = 123;
    pojo.b = 234876274;
    pojo.c = "HowAreYou?";
    pojo.d = null;

    operator.in.put(pojo);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expectedJSONString = "{\"a\":123,\"b\":234876274,\"c\":\"HowAreYou?\",\"d\":null,\"date\":null}";
    Assert.assertEquals(expectedJSONString, validDataSink.collectedTuples.get(0));
  }

  @Test
  public void testJSONToPOJOEmptyPOJO()
  {
    Test1Pojo pojo = new Test1Pojo();
    operator.in.put(pojo);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expectedJSONString = "{\"a\":0,\"b\":0,\"c\":null,\"d\":null,\"date\":null}";
    System.out.println(validDataSink.collectedTuples.get(0));
    Assert.assertEquals(expectedJSONString, validDataSink.collectedTuples.get(0));
  }

  @Test
  public void testJSONToPOJONullPOJO()
  {
    operator.in.put(null);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expectedJSONString = "null";
    Assert.assertEquals(expectedJSONString, validDataSink.collectedTuples.get(0));
  }

  @Test
  public void testJSONToPOJONoFieldPOJO()
  {
    operator.endWindow();
    operator.teardown();
    operator.setClazz(Test2Pojo.class);
    operator.setup(null);
    operator.beginWindow(1);

    Test2Pojo o = new Test2Pojo();
    operator.in.put(o);
    Assert.assertEquals(0, validDataSink.collectedTuples.size());
    Assert.assertEquals(1, invalidDataSink.collectedTuples.size());
    Assert.assertEquals(o, invalidDataSink.collectedTuples.get(0));
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

  public static class Test2Pojo
  {
  }

}
