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
package org.apache.apex.malhar.contrib.splunk;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;

/**
 *
 * Unit test for splunk input operator. The test, queries splunk server for 100 rows and checks
 * how many rows are returned.
 *
 */
public class SplunkInputOperatorTest
{
  public static final String HOST = "127.0.0.1";
  public static final int PORT = 8089;

  private static final String USER_NAME = "admin";
  private static final String PASSWORD = "rohit";
  private static String APP_ID = "SplunkTest";
  private static int OPERATOR_ID = 0;


  private static class TestInputOperator extends AbstractSplunkInputOperator<String>
  {
    private static final String retrieveQuery = "search * | head 100";

    @Override
    public String getTuple(String value)
    {
      return value;
    }

    @Override
    public String queryToRetrieveData()
    {
      return retrieveQuery;
    }

  }

  @Test
  public void TestSplunkInputOperator()
  {
    SplunkStore store = new SplunkStore();
    store.setHost(HOST);
    store.setPassword(PASSWORD);
    store.setPort(PORT);
    store.setUserName(USER_NAME);

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    TestInputOperator inputOperator = new TestInputOperator();
    inputOperator.setStore(store);
    inputOperator.setEarliestTime("-1000h");
    inputOperator.setLatestTime("now");
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(context);
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("rows from splunk", 100, sink.collectedTuples.size());
  }

}
