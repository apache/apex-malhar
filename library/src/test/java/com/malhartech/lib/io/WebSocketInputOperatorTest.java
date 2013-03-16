/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.daemon.Daemon;
import com.malhartech.engine.TestSink;
import com.malhartech.stream.SampleWebSocketPublisher;
import java.net.URI;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;

public class WebSocketInputOperatorTest
{
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testWebSocketInputModule() throws Exception
  {

    if (false) {
      // this is not working yet - start server manually for this test
      System.out.println("Starting Daemon...");
      Daemon.setPort(9090);
      Daemon.setLocalMode(true);
      Daemon.setup();
      Daemon.start();
    }

    String url = "ws://localhost:9090/channel/testChannel";
    final WebSocketInputOperator operator = new WebSocketInputOperator();

    TestSink<Map<String, String>> sink = new TestSink<Map<String, String>>();

    operator.outputPort.setSink(sink);
    operator.setName("testWebSocketInputNode");
    operator.setUrl(new URI(url));

    operator.setup(null);
    operator.activate(null);

    // start publisher after subscriber listens to ensure we don't miss the message
    SampleWebSocketPublisher sp = new SampleWebSocketPublisher();
    sp.setChannelUrl("http://localhost:9090/channel/testChannel");
    Thread t = new Thread(sp);
    t.start();


    int timeoutMillis = 2000;
    while (sink.collectedTuples.isEmpty() && timeoutMillis > 0) {
      operator.emitTuples();
      timeoutMillis -= 20;
      Thread.sleep(20);
    }

    Assert.assertTrue("tuple emitted", sink.collectedTuples.size() > 0);

    Map<String, String> tuple = sink.collectedTuples.get(0);
    Assert.assertEquals("", tuple.get("hello"), "world");

    operator.deactivate();
    operator.teardown();
    t.interrupt();
    t.join();
  }

}
