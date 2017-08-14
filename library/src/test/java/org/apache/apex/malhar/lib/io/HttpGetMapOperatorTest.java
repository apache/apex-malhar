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
package org.apache.apex.malhar.lib.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;

/**
 * Tests the {@link HttpGetMapOperator} for sending get requests to specified url
 * and processing responses if output port is connected
 */
public class HttpGetMapOperatorTest
{
  private final String KEY1 = "key1";
  private final String KEY2 = "key2";
  private final String VAL1 = "val1";
  private final String VAL2 = "val2";
  private final String VAL3 = "val3";
  private final String VAL4 = "val4";

  @Test
  public void testGetOperator() throws Exception
  {
    final List<Map<String, String[]>> receivedRequests = new ArrayList<Map<String, String[]>>();
    Handler handler = new AbstractHandler()
    {
      @Override
      public void handle(String string, Request rq, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
      {
        receivedRequests.add(request.getParameterMap());
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println(request.getParameterNames().nextElement());
        ((Request)request).setHandled(true);
      }

    };

    Server server = new Server(0);
    server.setHandler(handler);
    server.start();

    String url = "http://localhost:" + server.getConnectors()[0].getLocalPort() + "/context";

    HttpGetMapOperator<String, String> operator = new HttpGetMapOperator<String, String>();
    operator.setUrl(url);
    operator.setup(null);

    CollectorTestSink<String> sink = TestUtils.setSink(operator.output, new CollectorTestSink<String>());

    Map<String, String> data = new HashMap<String, String>();
    data.put(KEY1, VAL1);
    operator.input.process(data);

    data.clear();
    data.put(KEY1, VAL2);
    data.put(KEY2, VAL3);
    operator.input.process(data);

    data.clear();
    data.put("key1", VAL4);
    operator.input.process(data);

    long startTms = System.currentTimeMillis();
    long waitTime = 10000L;

    while (receivedRequests.size() < 3 && System.currentTimeMillis() - startTms < waitTime) {
      Thread.sleep(250);
    }

    Assert.assertEquals("request count", 3, receivedRequests.size());
    Assert.assertEquals("parameter value", VAL1, receivedRequests.get(0).get(KEY1)[0]);
    Assert.assertNull("parameter value", receivedRequests.get(0).get(KEY2));
    Assert.assertEquals("parameter value", VAL2, receivedRequests.get(1).get(KEY1)[0]);
    Assert.assertEquals("parameter value", VAL3, receivedRequests.get(1).get(KEY2)[0]);
    Assert.assertEquals("parameter value", VAL4, receivedRequests.get(2).get(KEY1)[0]);

    Assert.assertEquals("emitted size", 3, sink.collectedTuples.size());
    Assert.assertEquals("emitted tuples", KEY1, sink.collectedTuples.get(0).trim());
  }

}
