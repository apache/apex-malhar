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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONObject;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.io.IOUtils;

/**
 * Functional test for {@link org.apache.apex.malhar.lib.io.HttpPostOutputOperator}.
 */
public class HttpPostOutputOperatorTest
{
  boolean receivedMessage = false;

  @Test
  public void testHttpOutputNode() throws Exception
  {

    final List<String> receivedMessages = new ArrayList<String>();
    Handler handler = new AbstractHandler()
    {
      @Override
      @Consumes({MediaType.APPLICATION_JSON})
      public void handle(String string, Request rq, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
      {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(request.getInputStream(), bos);
        receivedMessages.add(new String(bos.toByteArray()));
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>Thanks</h1>");
        ((Request)request).setHandled(true);
        receivedMessage = true;
      }

    };

    Server server = new Server(0);
    server.setHandler(handler);
    server.start();

    String url = "http://localhost:" + server.getConnectors()[0].getLocalPort() + "/somecontext";

    HttpPostOutputOperator<Object> node = new HttpPostOutputOperator<Object>();
    node.setUrl(url);

    node.setup(null);

    Map<String, String> data = new HashMap<String, String>();
    data.put("somekey", "somevalue");
    node.input.process(data);

    // Wait till the message is received or a maximum timeout elapses
    int timeoutMillis = 10000;
    while (!receivedMessage && timeoutMillis > 0) {
      timeoutMillis -= 20;
      Thread.sleep(20);
    }

    Assert.assertEquals("number requests", 1, receivedMessages.size());
    JSONObject json = new JSONObject(data);
    Assert.assertTrue("request body " + receivedMessages.get(0), receivedMessages.get(0).contains(json.toString()));

    receivedMessages.clear();
    String stringData = "stringData";
    node.input.process(stringData);
    Assert.assertEquals("number requests", 1, receivedMessages.size());
    Assert.assertEquals("request body " + receivedMessages.get(0), stringData, receivedMessages.get(0));


    node.teardown();
    server.stop();
  }

}
