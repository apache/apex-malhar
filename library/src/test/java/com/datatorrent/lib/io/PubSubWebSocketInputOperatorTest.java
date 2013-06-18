/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.io;

import com.datatorrent.daemon.Daemon;
import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.io.PubSubWebSocketInputOperator;
import com.datatorrent.stram.util.SamplePubSubWebSocketPublisher;
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
    Daemon daemon = new Daemon();
    daemon.setLocalMode(true);
    daemon.setup("localhost:19090");
    daemon.start();

    URI uri = new URI("ws://localhost:19090/pubsub");
    final PubSubWebSocketInputOperator operator = new PubSubWebSocketInputOperator();
    TestSink sink = new TestSink();

    operator.outputPort.setSink(sink);
    operator.setName("testWebSocketInputNode");
    operator.setUri(uri);
    operator.addTopic("testTopic");

    operator.setup(null);
    operator.activate(null);

    // start publisher after subscriber listens to ensure we don't miss the message
    SamplePubSubWebSocketPublisher sp = new SamplePubSubWebSocketPublisher();
    sp.setUri(uri);
    Thread t = new Thread(sp);
    t.start();


    int timeoutMillis = 2000;
    while (sink.collectedTuples.isEmpty() && timeoutMillis > 0) {
      operator.emitTuples();
      timeoutMillis -= 20;
      Thread.sleep(20);
    }

    Assert.assertTrue("tuple emitted", sink.collectedTuples.size() > 0);

    Map<String, String> tuple = (Map<String, String>)sink.collectedTuples.get(0);
    Assert.assertEquals("Expects {\"hello\":\"world\"} as data", tuple.get("hello"), "world");

    operator.deactivate();
    operator.teardown();
    t.interrupt();
    t.join();
    daemon.stop();
  }

}
