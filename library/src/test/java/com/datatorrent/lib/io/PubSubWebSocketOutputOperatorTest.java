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

import com.datatorrent.lib.helper.SamplePubSubWebSocketServlet;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;
import com.datatorrent.lib.helper.SamplePubSubWebSocketSubscriber;
import static java.lang.Thread.sleep;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Test;

public class PubSubWebSocketOutputOperatorTest
{
  @Test
  public void testWebSocketOutputModule() throws Exception
  {
    Server server = new Server(new InetSocketAddress("localhost", 19091));
    SamplePubSubWebSocketServlet servlet = new SamplePubSubWebSocketServlet();
    ServletHolder sh = new ServletHolder(servlet);
    ServletContextHandler contextHandler = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
    contextHandler.addServlet(sh, "/pubsub");
    contextHandler.addServlet(sh, "/*");
    server.start();

    URI uri = new URI("ws://localhost:19091/pubsub");

    // start subscriber
    final SamplePubSubWebSocketSubscriber ss = new SamplePubSubWebSocketSubscriber();
    ss.setUri(uri);
    Thread t = new Thread(ss);
    t.start();

    final PubSubWebSocketOutputOperator<Map<String,String>> operator = new PubSubWebSocketOutputOperator<Map<String,String>>();
    operator.setTopic("testTopic");
    operator.setName("testWebSocketOutputNode");
    operator.setUri(uri);
    operator.setup(null);

    Map<String, String> data = new HashMap<String, String>();
    data.put("hello", "world");
    operator.beginWindow(1000);
    operator.input.process(data);
    operator.endWindow();

    boolean received = false;
    long timeoutMillis = 2000;
    long startMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() < (startMillis + timeoutMillis)) {
      if(ss.getMessagesReceived() > 0) {
        received = true;
        break;
      }
      sleep(50);
    }

    operator.teardown();

    Assert.assertTrue("Tuples received.", received);
    Map<String,String> o = (Map<String,String>)ss.getBuffer().get();
    Assert.assertEquals("Data expected to be {\"hello\":\"world\"}", o.get("hello"), "world");
    t.interrupt();
    t.join();
    server.stop();
  }

}
