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
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedMap;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Tests {@link AbstractHttpGetMultiValuedMapOperator} for sending get requests to specified url
 * and processing responses if output port is connected
 */
public class HttpMultiValuedMapGetOperatorTest
{
  private final String KEY1 = "key1";
  private final String KEY2 = "key2";
  private final String VAL1 = "val1";
  private final String VAL2 = "val2";
  private final String VAL3 = "val3";

  @Test
  public void testOperator() throws Exception
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

    TestHttpGetMultiValuedMapOperator operator = new TestHttpGetMultiValuedMapOperator();
    operator.setUrl(url);
    operator.setup(null);

    CollectorTestSink<String> sink = TestUtils.setSink(operator.output, new CollectorTestSink<String>());

    TestPojo pojo = new TestPojo();
    pojo.setName1(KEY1);
    pojo.setValue11(VAL1);
    pojo.setValue12(VAL2);
    pojo.setName2(KEY2);
    pojo.setValue21(VAL1);
    pojo.setValue22(VAL2);
    pojo.setValue23(VAL3);

    operator.input.process(pojo);

    long startTms = System.currentTimeMillis();
    long waitTime = 10000L;

    while (receivedRequests.size() < 3 && System.currentTimeMillis() - startTms < waitTime) {
      Thread.sleep(250);
    }

    Assert.assertEquals("request count", 1, receivedRequests.size());
    Assert.assertEquals("parameter key count", 2, receivedRequests.get(0).size());
    Assert.assertEquals("parameter value count", 2, receivedRequests.get(0).get(KEY1).length);
    Assert.assertEquals("parameter value count", 3, receivedRequests.get(0).get(KEY2).length);
    Assert.assertEquals("parameter value", VAL1, receivedRequests.get(0).get(KEY1)[0]);
    Assert.assertEquals("parameter value", VAL2, receivedRequests.get(0).get(KEY1)[1]);
    Assert.assertEquals("parameter value", VAL1, receivedRequests.get(0).get(KEY2)[0]);
    Assert.assertEquals("parameter value", VAL2, receivedRequests.get(0).get(KEY2)[1]);
    Assert.assertEquals("parameter value", VAL3, receivedRequests.get(0).get(KEY2)[2]);
    Assert.assertNull("parameter value", receivedRequests.get(0).get("randomkey"));


    Assert.assertEquals("emitted size", 1, sink.collectedTuples.size());
    MultivaluedMapImpl map = new MultivaluedMapImpl();
    map.add(pojo.getName1(), pojo.getValue11());
    map.add(pojo.getName1(), pojo.getValue12());
    map.add(pojo.getName2(), pojo.getValue21());
    map.add(pojo.getName2(), pojo.getValue22());
    map.add(pojo.getName2(), pojo.getValue23());
    Map.Entry<String,List<String>> entry = map.entrySet().iterator().next();
    Assert.assertEquals("emitted tuples", entry.getKey(), sink.collectedTuples.get(0).trim());
  }

  public static class TestHttpGetMultiValuedMapOperator extends AbstractHttpGetMultiValuedMapOperator<TestPojo, String>
  {
    @Override
    protected MultivaluedMap<String, String> getQueryParams(TestPojo input)
    {
      MultivaluedMapImpl map = new MultivaluedMapImpl();

      map.add(input.getName1(), input.getValue11());
      map.add(input.getName1(), input.getValue12());
      map.add(input.getName2(), input.getValue21());
      map.add(input.getName2(), input.getValue22());
      map.add(input.getName2(), input.getValue23());

      return map;
    }

    @Override
    protected void processResponse(ClientResponse response)
    {
      output.emit(response.getEntity(String.class));
    }

  }

  public static class TestPojo
  {
    private String name1;
    private String value11;
    private String value12;
    private String name2;
    private String value21;
    private String value22;
    private String value23;

    public TestPojo()
    {
    }

    public String getName1()
    {
      return name1;
    }

    public void setName1(String name1)
    {
      this.name1 = name1;
    }

    public String getValue11()
    {
      return value11;
    }

    public void setValue11(String value11)
    {
      this.value11 = value11;
    }

    public String getValue12()
    {
      return value12;
    }

    public void setValue12(String value12)
    {
      this.value12 = value12;
    }

    public String getName2()
    {
      return name2;
    }

    public void setName2(String name2)
    {
      this.name2 = name2;
    }

    public String getValue21()
    {
      return value21;
    }

    public void setValue21(String value21)
    {
      this.value21 = value21;
    }

    public String getValue22()
    {
      return value22;
    }

    public void setValue22(String value22)
    {
      this.value22 = value22;
    }

    public String getValue23()
    {
      return value23;
    }

    public void setValue23(String value23)
    {
      this.value23 = value23;
    }

  }

}
