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

import java.util.List;
import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

import com.google.common.collect.Lists;

/**
 *
 * Unit test for splunk tcp output operator. The test sends 10 values to the splunk server and then
 * queries it for last 10 rows to check if the values are same or not.
 *
 */
public class SplunkTcpOutputOperatorTest
{
  public static final String HOST = "127.0.0.1";
  public static final int PORT = 8089;
  private static final String USER_NAME = "admin";
  private static final String PASSWORD = "rohit";


  private static class TestInputOperator extends AbstractSplunkInputOperator<String>
  {
    private static final String retrieveQuery = "search * | head 10";

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
  public void TestSplunkTcpOutputOperator()
  {
    int flag = 1;
    SplunkStore store = new SplunkStore();
    store.setHost(HOST);
    store.setPassword(PASSWORD);
    store.setPort(PORT);
    store.setUserName(USER_NAME);

    SplunkTcpOutputOperator<Integer> outputOperator = new SplunkTcpOutputOperator<Integer>();
    outputOperator.setTcpPort("9999");
    outputOperator.setStore(store);
    outputOperator.setup(null);

    List<Integer> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new Integer(i));
    }

    outputOperator.beginWindow(0);
    for (Integer event : events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();

    TestInputOperator inputOperator = new TestInputOperator();
    inputOperator.setStore(store);
    inputOperator.setEarliestTime("-1000h");
    inputOperator.setLatestTime("now");
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(null);
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    List<Object> collectedEvents = sink.collectedTuples;
    if (events.size() != collectedEvents.size()) {
      flag = 0;
    }

    if (flag == 1) {
      if (collectedEvents.get(0).toString().length() == 1) {
        for (int i = 0; i < collectedEvents.size(); i++) {
          if (!events.contains(Integer.parseInt(collectedEvents.get(i).toString()))) {
            flag = 0;
            break;
          }
        }
      } else if (collectedEvents.get(0).toString().length() == 10) {
        for (int i = 0; i < 10; i++) {
          if (!collectedEvents.get(0).toString().contains(events.get(i).toString())) {
            flag = 0;
            break;
          }
        }
      }

    }

    Assert.assertEquals("Check if last 10 values are the same or not", 1, flag);
  }

}
