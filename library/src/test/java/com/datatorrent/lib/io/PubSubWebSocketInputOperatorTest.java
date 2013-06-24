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
 * limitations under the License. See accompanying LICENSE file.
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
