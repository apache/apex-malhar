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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.codehaus.jettison.json.JSONException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.appdata.schemas.SchemaUtils;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;

public class RegexParserTest
{
  RegexParser regex = new RegexParser();
  private CollectorTestSink<Object> error = new CollectorTestSink<Object>();

  private CollectorTestSink<Object> pojoPort = new CollectorTestSink<Object>();

  @Rule
  public Watcher watcher = new Watcher();

  public class Watcher extends TestWatcher
  {
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      regex.err.setSink(error);
      regex.out.setSink(pojoPort);
      regex.setSchema(SchemaUtils.jarResourceFileToString("RegexSplitterschema.json"));
      regex.setSplitRegexPattern(".+\\[SEQ=\\w+\\]\\s*(\\d+:[\\d\\d:]+)\\s(\\d+)\\s*(.+)");
      regex.setClazz(ServerLog.class);
      regex.setup(null);
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
    }

  }

  @Test
  public void TestValidInputCase() throws ParseException
  {
    regex.beginWindow(0);
    String line = "2015-10-01T03:14:49.000-07:00 lvn-d1-dev DevServer[9876]: INFO: [EVENT][SEQ=248717]" +
        " 2015:10:01:03:14:49 101 sign-in_id=11111@psop.com ip_address=1.1.1.1  service_id=IP1234-NPB12345_00 " +
        "result=RESULT_SUCCESconsole_id=0000000138e91b4e58236bf32besdafasdfasdfasdfsadf  account_id=11111  platform=pik";
    regex.in.process(line.getBytes());
    regex.endWindow();
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(ServerLog.class, obj.getClass());
    ServerLog pojo = (ServerLog)obj;
    Assert.assertEquals(101, pojo.getId());
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy:MM:dd:hh:mm:ss");
    Date date = sdf.parse("2015:10:01:03:14:49");
    Assert.assertEquals(date, pojo.getDate());
    Assert.assertEquals("sign-in_id=11111@psop.com ip_address=1.1.1.1  service_id=IP1234-NPB12345_00"
        + " result=RESULT_SUCCESconsole_id=0000000138e91b4e58236bf32besdafasdfasdfasdfsadf  account_id=11111  " +
        "platform=pik", pojo.getMessage());
    Assert.assertEquals(1, regex.getIncomingTuplesCount());
    Assert.assertEquals(1, regex.getEmittedObjectCount());
  }

  @Test
  public void testEmptyInput() throws JSONException
  {
    String tuple = "";
    regex.beginWindow(0);
    regex.in.process(tuple.getBytes());
    regex.endWindow();
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    Assert.assertEquals(1, regex.getIncomingTuplesCount());
    Assert.assertEquals(1, regex.getErrorTupleCount());
  }

  @Test
  public void TestInValidDateInputCase() throws ParseException
  {
    regex.beginWindow(0);
    String line = "2015-10-01T03:14:49.000-07:00 lvn-d1-dev DevServer[9876]: INFO: [EVENT][SEQ=248717]"
        + " qwerty 101 sign-in_id=11111@psop.com ip_address=1.1.1.1  service_id=IP1234-NPB12345_00"
        + " result=RESULT_SUCCESconsole_id=0000000138e91b4e58236bf32besdafasdfasdfasdfsadf  account_id=11111  platform=pik";
    regex.in.process(line.getBytes());
    regex.endWindow();
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String, String> obj = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals("2015-10-01T03:14:49.000-07:00 lvn-d1-dev DevServer[9876]: INFO: [EVENT][SEQ=248717]" +
        " qwerty 101 sign-in_id=11111@psop.com ip_address=1.1.1.1  service_id=IP1234-NPB12345_00 " +
        "result=RESULT_SUCCESconsole_id=0000000138e91b4e58236bf32besdafasdfasdfasdfsadf  account_id=11111  platform=pik", obj.getKey());
    Assert.assertEquals("The incoming tuple do not match with the Regex pattern defined.", obj.getValue());
    Assert.assertEquals(1, regex.getIncomingTuplesCount());
    Assert.assertEquals(0, regex.getEmittedObjectCount());
    Assert.assertEquals(1, regex.getErrorTupleCount());
  }

  @Test
  public void TestInValidIntInputCase() throws ParseException
  {
    regex.beginWindow(0);
    String line = "2015-10-01T03:14:49.000-07:00 lvn-d1-dev DevServer[9876]: INFO: [EVENT][SEQ=248717] " +
        "2015:10:01:03:14:46 hskhhskfk sign-in_id=11111@psop.com ip_address=1.1.1.1  service_id=IP1234-NPB12345_00 " +
        "result=RESULT_SUCCESconsole_id=0000000138e91b4e58236bf32besdafasdfasdfasdfsadf  account_id=11111  platform=pik";
    regex.in.process(line.getBytes());
    regex.endWindow();
    KeyValPair<String, String> obj = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals("2015-10-01T03:14:49.000-07:00 lvn-d1-dev DevServer[9876]: INFO: [EVENT][SEQ=248717] " +
        "2015:10:01:03:14:46 hskhhskfk sign-in_id=11111@psop.com ip_address=1.1.1.1  service_id=IP1234-NPB12345_00 " +
        "result=RESULT_SUCCESconsole_id=0000000138e91b4e58236bf32besdafasdfasdfasdfsadf  account_id=11111  platform=pik", obj.getKey());
    Assert.assertEquals("The incoming tuple do not match with the Regex pattern defined.", obj.getValue());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    Assert.assertEquals(1, regex.getIncomingTuplesCount());
    Assert.assertEquals(0, regex.getEmittedObjectCount());
    Assert.assertEquals(1, regex.getErrorTupleCount());
  }

  @Test
  public void testNullInput() throws JSONException
  {
    regex.beginWindow(0);
    regex.in.process(null);
    regex.endWindow();
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    Assert.assertEquals(1, regex.getIncomingTuplesCount());
    Assert.assertEquals(1, regex.getErrorTupleCount());
  }

  @Test
  public void testParserValidInputMetricVerification()
  {
    regex.beginWindow(0);
    Assert.assertEquals(0, regex.getIncomingTuplesCount());
    Assert.assertEquals(0, regex.getErrorTupleCount());
    Assert.assertEquals(0, regex.getEmittedObjectCount());
    String tuple = "2015-10-01T03:14:49.000-07:00 lvn-d1-dev DevServer[9876]: INFO: [EVENT][SEQ=248717] " +
        "2015:10:01:03:14:49 101 sign-in_id=11111@psop.com ip_address=1.1.1.1  service_id=IP1234-NPB12345_00 " +
        "result=RESULT_SUCCESconsole_id=0000000138e91b4e58236bf32besdafasdfasdfasdfsadf  account_id=11111  platform=pik";
    regex.in.process(tuple.getBytes());
    regex.endWindow();
    Assert.assertEquals(1, regex.getIncomingTuplesCount());
    Assert.assertEquals(0, regex.getErrorTupleCount());
    Assert.assertEquals(1, regex.getEmittedObjectCount());
  }

  @Test
  public void testParserInvalidInputMetricVerification()
  {
    regex.beginWindow(0);
    Assert.assertEquals(0, regex.getIncomingTuplesCount());
    Assert.assertEquals(0, regex.getErrorTupleCount());
    Assert.assertEquals(0, regex.getEmittedObjectCount());
    String tuple = "{" + "\"id\": 2" + "}";
    regex.in.process(tuple.getBytes());
    regex.endWindow();
    Assert.assertEquals(1, regex.getIncomingTuplesCount());
    Assert.assertEquals(1, regex.getErrorTupleCount());
    Assert.assertEquals(0, regex.getEmittedObjectCount());
  }

  @Test
  public void testParserMetricResetVerification()
  {
    Assert.assertEquals(0, regex.getIncomingTuplesCount());
    Assert.assertEquals(0, regex.getErrorTupleCount());
    Assert.assertEquals(0, regex.getEmittedObjectCount());
    String tuple = "2015-10-01T03:14:49.000-07:00 lvn-d1-dev DevServer[9876]: INFO: [EVENT][SEQ=248717] " +
        "2015:10:01:03:14:49 101 sign-in_id=11111@psop.com ip_address=1.1.1.1  service_id=IP1234-NPB12345_00 " +
        "result=RESULT_SUCCESconsole_id=0000000138e91b4e58236bf32besdafasdfasdfasdfsadf  account_id=11111  platform=pik";
    regex.beginWindow(0);
    regex.in.process(tuple.getBytes());
    regex.endWindow();
    Assert.assertEquals(1, regex.getIncomingTuplesCount());
    Assert.assertEquals(0, regex.getErrorTupleCount());
    Assert.assertEquals(1, regex.getEmittedObjectCount());
    regex.beginWindow(1);
    Assert.assertEquals(0, regex.getIncomingTuplesCount());
    Assert.assertEquals(0, regex.getErrorTupleCount());
    Assert.assertEquals(0, regex.getEmittedObjectCount());
    regex.in.process(tuple.getBytes());
    Assert.assertEquals(1, regex.getIncomingTuplesCount());
    Assert.assertEquals(0, regex.getErrorTupleCount());
    Assert.assertEquals(1, regex.getEmittedObjectCount());
    regex.endWindow();
  }

  public static class ServerLog
  {
    private String message;
    private Date date;
    private int id;

    public String getMessage()
    {
      return message;
    }

    public void setMessage(String message)
    {
      this.message = message;
    }

    public int getId()
    {
      return id;
    }

    public void setId(int id)
    {
      this.id = id;
    }

    public Date getDate()
    {
      return date;
    }

    public void setDate(Date date)
    {
      this.date = date;
    }

  }

}
