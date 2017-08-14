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
package org.apache.apex.malhar.lib.formatter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.io.fs.AbstractFileOutputOperatorTest.FSTestWatcher;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.apex.malhar.lib.util.TestUtils;
import org.apache.apex.malhar.lib.util.TestUtils.TestInfo;
import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
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
    Ad pojo = new Ad();
    pojo.adId = 123;
    pojo.campaignId = 234876274;
    pojo.description = "sports";
    pojo.sizes = Lists.newArrayList("200x350", "600x800");
    pojo.startDate = new DateTime().withDate(2016, 1, 1).withMillisOfDay(0).withZoneRetainFields(DateTimeZone.UTC)
        .toDate();
    pojo.endDate = new DateTime().withDate(2016, 2, 1).withMillisOfDay(0).withZoneRetainFields(DateTimeZone.UTC)
        .toDate();
    operator.in.put(pojo);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expectedJSONString = "{\"adId\":123,\"campaignId\":234876274,\"sizes\":[\"200x350\",\"600x800\"],\"startDate\":\"Fri, 1 Jan 2016 00:00:00\",\"endDate\":\"01-Feb-2016\",\"desc\":\"sports\"}";
    Assert.assertEquals(expectedJSONString, validDataSink.collectedTuples.get(0));
    Assert.assertEquals(1, operator.getIncomingTuplesCount());
    Assert.assertEquals(1, operator.getEmittedObjectCount());
    Assert.assertEquals(0, operator.getErrorTupleCount());
  }

  @Test
  public void testJSONToPOJONullFields()
  {
    Ad pojo = new Ad();
    pojo.adId = 123;
    pojo.campaignId = 234876274;
    pojo.description = "sports";
    pojo.sizes = null;
    pojo.startDate = null;
    pojo.endDate = null;

    operator.in.put(pojo);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expectedJSONString = "{\"adId\":123,\"campaignId\":234876274,\"sizes\":null,\"startDate\":null,\"endDate\":null,\"desc\":\"sports\"}";
    Assert.assertEquals(expectedJSONString, validDataSink.collectedTuples.get(0));
    Assert.assertEquals(1, operator.getIncomingTuplesCount());
    Assert.assertEquals(1, operator.getEmittedObjectCount());
    Assert.assertEquals(0, operator.getErrorTupleCount());
  }

  @Test
  public void testJSONToPOJOEmptyPOJO()
  {
    Ad pojo = new Ad();
    operator.in.put(pojo);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expectedJSONString = "{\"adId\":0,\"campaignId\":0,\"sizes\":null,\"startDate\":null,\"endDate\":null,\"desc\":null}";
    Assert.assertEquals(expectedJSONString, validDataSink.collectedTuples.get(0));
    Assert.assertEquals(1, operator.getIncomingTuplesCount());
    Assert.assertEquals(1, operator.getEmittedObjectCount());
    Assert.assertEquals(0, operator.getErrorTupleCount());
  }

  @Test
  public void testJSONToPOJONullPOJO()
  {
    operator.in.put(null);
    Assert.assertEquals(0, validDataSink.collectedTuples.size());
    Assert.assertEquals(1, invalidDataSink.collectedTuples.size());
    Assert.assertEquals(1, operator.getIncomingTuplesCount());
    Assert.assertEquals(0, operator.getEmittedObjectCount());
    Assert.assertEquals(1, operator.getErrorTupleCount());
  }

  @Test
  public void testJSONToPOJONoFieldPOJO()
  {
    operator.endWindow();
    operator.teardown();
    operator.setup(null);
    operator.beginWindow(1);

    TestPojo o = new TestPojo();
    operator.in.put(o);
    Assert.assertEquals(0, validDataSink.collectedTuples.size());
    Assert.assertEquals(1, invalidDataSink.collectedTuples.size());
    Assert.assertEquals(o, invalidDataSink.collectedTuples.get(0));
    Assert.assertEquals(1, operator.getIncomingTuplesCount());
    Assert.assertEquals(0, operator.getEmittedObjectCount());
    Assert.assertEquals(1, operator.getErrorTupleCount());
  }

  @Test
  public void testOperatorSerialization()
  {
    Assert.assertNotNull("Serialization", KryoCloneUtils.cloneObject(this.operator));
  }

  public static class Ad
  {
    public int adId;
    public long campaignId;
    @JsonProperty("desc")
    public String description;
    public List<String> sizes;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "EEE, d MMM yyyy HH:mm:ss")
    public Date startDate;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MMM-yyyy")
    public Date endDate;
  }

  public static class TestPojo
  {
  }

}
