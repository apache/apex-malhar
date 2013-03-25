/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.daemon.Daemon;
import com.malhartech.stream.SamplePubSubWebSocketPublisher;
import com.malhartech.stream.SamplePubSubWebSocketSubscriber;
import com.malhartech.stream.StramTestSupport;
import com.malhartech.stream.StramTestSupport.WaitCondition;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;

public class PubSubWebSocketOutputOperatorTest
{
  @Test
  public void testWebSocketOutputModule() throws Exception
  {
    System.out.println("Starting Daemon...");
    Daemon.setLocalMode(true);
    Daemon.setup("localhost:19090");
    Daemon.start();

    URI uri = new URI("ws://localhost:19090/pubsub");

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

    WaitCondition c = new WaitCondition() {

      @Override
      public boolean isComplete()
      {
        return ss.getMessagesReceived() > 0;
      }
    };

    operator.teardown();

    Assert.assertTrue("Tuples received.", StramTestSupport.awaitCompletion(c, 2000));
    Map<String,String> o = (Map<String,String>)ss.getBuffer().get();
    Assert.assertEquals("Data expected to be {\"hello\":\"world\"}", o.get("hello"), "world");
    t.interrupt();
    t.join();
  }

}
