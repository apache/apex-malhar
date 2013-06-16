/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
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
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Test;

import com.datatorrent.lib.io.HttpOutputOperator;


public class HttpOutputOperatorTest {

  @Test
  public void testHttpOutputNode() throws Exception {

    final List<String> receivedMessages = new ArrayList<String>();
    Handler handler=new AbstractHandler()
    {
      @Override
      public void handle(String string, Request rq, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
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
