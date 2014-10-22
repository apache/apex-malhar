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
package com.datatorrent.demos.mrmonitor;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Test;

import com.datatorrent.api.LocalMode;

import com.datatorrent.lib.helper.SamplePubSubWebSocketServlet;

/**
 * <p>MapReduceDebuggerApplicationTest class.</p>
 *
 * @since 0.3.4
 */

public class MrMonitoringApplicationTest
{

  @Test
  public void testApplication() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.addResource("dt-site-monitoring.xml");
    String[] connection = conf.get("dt.attr.GATEWAY_CONNECT_ADDRESS").split(":");
    Server server = new Server(new InetSocketAddress(connection[0], Integer.parseInt(connection[1])));
    SamplePubSubWebSocketServlet servlet = new SamplePubSubWebSocketServlet();
    ServletHolder sh = new ServletHolder(servlet);
    ServletContextHandler contextHandler = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
    contextHandler.addServlet(sh, "/pubsub");
    contextHandler.addServlet(sh, "/*");
    server.start();
    MRMonitoringApplication application = new MRMonitoringApplication();
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(application, conf);
    LocalMode.Controller lc = lma.getController();
    lc.run(10000);
    server.stop();
  }

}
