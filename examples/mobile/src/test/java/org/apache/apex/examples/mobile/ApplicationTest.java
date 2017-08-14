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
package org.apache.apex.examples.mobile;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.Servlet;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.helper.SamplePubSubWebSocketServlet;
import org.apache.apex.malhar.lib.io.PubSubWebSocketInputOperator;
import org.apache.apex.malhar.lib.io.PubSubWebSocketOutputOperator;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.utils.PubSubHelper;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

public class ApplicationTest
{
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);

  public ApplicationTest()
  {
  }

  /**
   * Test of getApplication method, of class Application.
   */
  @Test
  public void testGetApplication() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.addResource("dt-site-mobile.xml");
    Server server = new Server(0);
    Servlet servlet = new SamplePubSubWebSocketServlet();
    ServletHolder sh = new ServletHolder(servlet);
    ServletContextHandler contextHandler = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
    contextHandler.addServlet(sh, "/pubsub");
    contextHandler.addServlet(sh, "/*");
    server.start();
    Connector[] connector = server.getConnectors();
    conf.set("dt.attr.GATEWAY_CONNECT_ADDRESS", "localhost:" + connector[0].getLocalPort());
    URI uri = PubSubHelper.getURI("localhost:" + connector[0].getLocalPort());

    PubSubWebSocketOutputOperator<Object> outputOperator = new PubSubWebSocketOutputOperator<Object>();
    outputOperator.setUri(uri);
    outputOperator.setTopic(conf.get("dt.application.MobileExample.operator.QueryLocation.topic"));

    PubSubWebSocketInputOperator<Map<String, String>> inputOperator = new PubSubWebSocketInputOperator<Map<String, String>>();
    inputOperator.setUri(uri);
    inputOperator.setTopic(conf.get("dt.application.MobileExample.operator.LocationResults.topic"));

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    Map<String, String> data = new HashMap<String, String>();
    data.put("command", "add");
    data.put("phone", "5559990");

    Application app = new Application();
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();
    Thread.sleep(5000);
    inputOperator.setup(null);
    outputOperator.setup(null);
    inputOperator.activate(null);
    outputOperator.beginWindow(0);
    outputOperator.input.process(data);
    outputOperator.endWindow();
    inputOperator.beginWindow(0);
    int timeoutMillis = 5000;
    while (sink.collectedTuples.size() < 5 && timeoutMillis > 0) {
      inputOperator.emitTuples();
      timeoutMillis -= 20;
      Thread.sleep(20);
    }
    inputOperator.endWindow();
    lc.shutdown();
    inputOperator.teardown();
    outputOperator.teardown();
    server.stop();
    Assert.assertTrue("size of output is 5 ", sink.collectedTuples.size() == 5);
    for (Object obj : sink.collectedTuples) {
      Assert.assertEquals("Expected phone number", "5559990", ((Map<String, String>)obj).get("phone"));
    }
  }
}
