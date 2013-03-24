/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.daemon.Daemon;
import com.malhartech.engine.TestSink;
import com.malhartech.stream.SamplePubSubWebSocketPublisher;
import java.net.URI;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;

public class PubSubWebSocketInputOperatorTest
{
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testWebSocketInputModule() throws Exception
  {
    System.out.println("Starting Daemon...");
    Daemon.setLocalMode(true);
    Daemon.setup("localhost:19090");
    Daemon.start();

    String url = "ws://localhost:19090/pubsub";
    final PubSubWebSocketInputOperator operator = new PubSubWebSocketInputOperator();
    operator.addTopic("testTopic");
    TestSink<Map<String, String>> sink = new TestSink<Map<String, String>>();

    operator.outputPort.setSink(sink);
    operator.setName("testWebSocketInputNode");
    operator.setUri(new URI(url));

    operator.setup(null);
    operator.activate(null);

    // start publisher after subscriber listens to ensure we don't miss the message
    SamplePubSubWebSocketPublisher sp = new SamplePubSubWebSocketPublisher();
    sp.setChannelUrl(url);
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
    Assert.assertEquals("Expects {\"hello\":\"world\"} as data", tuple.get("hello"), "world");

    operator.deactivate();
    operator.teardown();
    t.interrupt();
    t.join();
  }

}
