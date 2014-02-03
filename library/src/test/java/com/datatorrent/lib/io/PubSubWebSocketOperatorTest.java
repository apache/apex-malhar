/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Test;

import com.datatorrent.lib.helper.SamplePubSubWebSocketServlet;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.concurrent.TimeoutException;

/**
 * Tests for {@link com.datatorrent.lib.io.PubSubWebSocketInputOperatorTest} {@link com.datatorrent.lib.io.PubSubWebSocketOutputOperatorTest}
 */
public class PubSubWebSocketOperatorTest
{

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testPubSubWebSocket() throws Exception {
    Server server = new Server(new InetSocketAddress("localhost", 19090));
    SamplePubSubWebSocketServlet servlet = new SamplePubSubWebSocketServlet();
    ServletHolder sh = new ServletHolder(servlet);
    ServletContextHandler contextHandler = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
    contextHandler.addServlet(sh, "/pubsub");
    contextHandler.addServlet(sh, "/*");
    server.start();

    URI uri = new URI("ws://localhost:19090/pubsub");

    PubSubWebSocketOutputOperator<Map<String, String>> outputOperator = new PubSubWebSocketOutputOperator<Map<String, String>>();
    outputOperator.setName("testOutputOperator");
    outputOperator.setUri(uri);
    outputOperator.setTopic("testTopic");

    PubSubWebSocketInputOperator inputOperator = new PubSubWebSocketInputOperator();
    inputOperator.setName("testInputOperator");
    inputOperator.setUri(uri);
    inputOperator.addTopic("testTopic");

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(null);
    outputOperator.setup(null);

    inputOperator.activate(null);

    long timeout = System.currentTimeMillis() + 3000;
    while (!servlet.hasSubscriber()) {
      Thread.sleep(10);
      if (System.currentTimeMillis() > timeout) {
        throw new TimeoutException("No subscribers connected after 3 seconds");
      }
    }

    inputOperator.beginWindow(1000);
    outputOperator.beginWindow(1000);

    Map<String, String> data = new HashMap<String, String>();
    data.put("hello", "world");
    outputOperator.input.process(data);

    int timeoutMillis = 2000;
    while (sink.collectedTuples.isEmpty() && timeoutMillis > 0) {
      inputOperator.emitTuples();
      timeoutMillis -= 20;
      Thread.sleep(20);
    }

    outputOperator.endWindow();
    inputOperator.endWindow();

    Assert.assertTrue("tuple emitted", sink.collectedTuples.size() > 0);

    @SuppressWarnings("unchecked")
    Map<String, String> tuple = (Map<String, String>)sink.collectedTuples.get(0);
    Assert.assertEquals("Expects {\"hello\":\"world\"} as data", tuple.get("hello"), "world");

    inputOperator.deactivate();

    outputOperator.teardown();
    inputOperator.teardown();

    server.stop();

  }
}
