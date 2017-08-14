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

import org.codehaus.jettison.json.JSONException;
import org.jooq.exception.IOException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.appdata.schemas.SchemaUtils;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

public class LogParserTest
{
  private String filename = "logSchema.json";

  LogParser logParser = new LogParser();

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
      logParser.err.setSink(error);
      logParser.parsedOutput.setSink(pojoPort);
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      error.clear();
      pojoPort.clear();
      logParser.teardown();
    }
  }

  @Test
  public void TestEmptyInput()
  {
    String tuple = "";
    logParser.beginWindow(0);
    logParser.in.process(tuple.getBytes());
    logParser.endWindow();
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestNullInput()
  {
    logParser.beginWindow(0);
    logParser.in.process(null);
    logParser.endWindow();
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestSchemaInput() throws JSONException, java.io.IOException
  {
    logParser.setLogFileFormat(SchemaUtils.jarResourceFileToString(filename));
    logParser.setup(null);
    logParser.setClazz(LogSchema.class);
    logParser.setLogSchemaDetails(new LogSchemaDetails(logParser.geLogFileFormat()));
    String log = "125.125.125.125 smith 200 1043";
    logParser.beginWindow(0);
    logParser.in.process(log.getBytes());
    logParser.endWindow();
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    LogSchema pojo = (LogSchema)obj;
    Assert.assertEquals("125.125.125.125", pojo.getHost());
    Assert.assertEquals("smith", pojo.getUserName());
    Assert.assertEquals("200", pojo.getStatusCode());
    Assert.assertEquals("1043", pojo.getBytes());
  }

  @Test
  public void TestInvalidSchemaInput() throws JSONException, IOException
  {
    logParser.setLogFileFormat(SchemaUtils.jarResourceFileToString("invalidLogSchema.json"));
    logParser.setup(null);
    logParser.setClazz(LogSchema.class);
    logParser.setLogSchemaDetails(new LogSchemaDetails(logParser.geLogFileFormat()));
    String log = "125.125.125.125 smith 200 1043";
    logParser.beginWindow(0);
    logParser.in.process(log.getBytes());
    logParser.endWindow();
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  public static class LogSchema
  {
    private String host;
    private String userName;
    private String statusCode;
    private String bytes;

    public String getHost()
    {
      return host;
    }

    public void setHost(String host)
    {
      this.host = host;
    }

    public String getUserName()
    {
      return userName;
    }

    public void setUserName(String username)
    {
      this.userName = username;
    }

    public String getStatusCode()
    {
      return statusCode;
    }

    public void setStatusCode(String statusCode)
    {
      this.statusCode = statusCode;
    }

    public String getBytes()
    {
      return bytes;
    }

    public void setBytes(String bytes)
    {
      this.bytes = bytes;
    }

    @Override
    public String toString()
    {
      return "LogSchema [host=" + host + ", userName=" + userName
        + ", statusCode=" + statusCode + ", bytes=" + bytes + "]";
    }
  }
}
