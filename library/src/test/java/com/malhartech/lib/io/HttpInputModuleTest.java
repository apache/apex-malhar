/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import junit.framework.Assert;

import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.ModuleContext;
import com.malhartech.dag.TestSink;


public class HttpInputModuleTest {

  @Test
  public void testHttpInputNode() throws Exception {

    final List<String> receivedMessages = new ArrayList<String>();
    Handler handler=new AbstractHandler()
    {
        @Override
        public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch)
            throws IOException, ServletException
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            IOUtils.copy(request.getInputStream(), bos);
            receivedMessages.add(new String(bos.toByteArray()));
            //response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);

            try {
              JSONObject json = new JSONObject();
              json.put("responseId", "response1");
              response.getOutputStream().println(json.toString());
              response.setContentType("application/json"); // set after getOutputStream to avoid ;charset=.. appended
            } catch (JSONException e) {
              response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error generating response: " + e.toString());
            }

            ((Request)request).setHandled(true);
        }
    };

    Server server = new Server(0);
    server.setHandler(handler);
    server.start();

    String url = "http://localhost:" + server.getConnectors()[0].getLocalPort() + "/somecontext";

    final HttpInputModule node = new HttpInputModule();

    TestSink<Map<String, Object>> sink = new TestSink<Map<String, Object>>();

    node.connect(HttpInputModule.OUTPUT, sink);

    final ModuleConfiguration config = new ModuleConfiguration("testHttpInputNode", Collections.<String,String>emptyMap());
    config.set(HttpOutputModule.P_RESOURCE_URL, url);

    node.setup(config);

    Thread nodeThread = new Thread()
    {
      @Override
      public void run()
      {
        node.activate(new ModuleContext(node.getId(), this));
      }
    };
    nodeThread.start();

    Thread.yield();
    while (nodeThread.getState() != Thread.State.RUNNABLE) {
      System.out.println("Waiting for node activation: " + nodeThread.getState());
      Thread.sleep(10);
    }

    long timeoutMillis = 3000;
    while (timeoutMillis > 0) {
      node.process(null);
      timeoutMillis -= 20;
      Thread.sleep(20);
    }

    Assert.assertTrue("tuple emmitted", sink.collectedTuples.size() > 0);

    Map<String, Object> tuple = sink.collectedTuples.get(0);
    Assert.assertEquals("", tuple.get("responseId"), "response1");

    node.teardown();
    server.stop();

  }

}
