/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.junit.Test;

import com.datatorrent.lib.io.HttpOutputOperator;


public class HttpOutputOperatorTest {

  @Test
  public void testHttpOutputNode() throws Exception {

    final List<String> receivedMessages = new ArrayList<String>();
    Handler handler=new AbstractHandler()
    {
      @Override
      public void handle(String string, HttpServletRequest request, HttpServletResponse response, int i) throws IOException, ServletException
      {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(request.getInputStream(), bos);
        receivedMessages.add(new String(bos.toByteArray()));
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>Thanks</h1>");
        ((Request)request).setHandled(true);
      }
    };

    Server server = new Server(0);
    server.setHandler(handler);
    server.start();

    String url = "http://localhost:" + server.getConnectors()[0].getLocalPort() + "/somecontext";
    System.out.println("url: " + url);


    HttpOutputOperator<Object> node = new HttpOutputOperator<Object>();
    node.setResourceURL(new URI(url));

    node.setup(null);

    Map<String, String> data = new HashMap<String, String>();
    data.put("somekey", "somevalue");
    node.input.process(data);

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
