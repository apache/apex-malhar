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
package com.datatorrent.contrib.mqtt;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Context.OperatorContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MqttOutputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(MqttOutputOperatorTest.class);
  private static Map<String, String> sendingData = new HashMap<String, String>();
  private static Map<String, String> receivedData = new HashMap<String, String>();
  static int sentTuples = 0;
  final static int totalTuples = 9;

  private static final class TestMqttOutputOperator extends AbstractSinglePortMqttOutputOperator<Map<String, String>>
  {
    @Override
    public void processTuple(Map<String, String> tuple)
    {
      for (Map.Entry<String, String> entry : tuple.entrySet()) {
        try {
          connection.publish(entry.getKey(), entry.getValue().getBytes(), QoS.AT_LEAST_ONCE, false);
        }
        catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    }

    public class GetDataThread extends Thread
    {
      @Override
      public void run()
      {
        try {
          int i = 0;
          Topic[] topics = new Topic[sendingData.size()];
          for (String key : sendingData.keySet()) {
            topics[i++] = new Topic(key, QoS.AT_MOST_ONCE);
          }
          connection.subscribe(topics);
          while (receivedData.size() < sendingData.size()) {
            Message msg = connection.receive();
            receivedData.put(msg.getTopic(), new String(msg.getPayload()));
          }
        }
        catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }

    }

  }

  public static class SourceModule extends BaseOperator
          implements InputOperator, ActivationListener<OperatorContext>
  {
    public final transient DefaultOutputPort<Map<String, String>> outPort = new DefaultOutputPort<Map<String, String>>();
    static transient ArrayBlockingQueue<Map<String, String>> holdingBuffer;

    @Override
    public void setup(OperatorContext context)
    {
      holdingBuffer = new ArrayBlockingQueue<Map<String, String>>(1024 * 1024);
    }

    public void emitTuple(Map<String, String> message)
    {
      outPort.emit(message);
    }

    @Override
    public void emitTuples()
    {
      for (int i = holdingBuffer.size(); i-- > 0;) {
        emitTuple(holdingBuffer.poll());
      }
    }

    @Override
    public void activate(OperatorContext ctx)
    {
      for (Map.Entry<String, String> e : sendingData.entrySet()) {
        Map<String, String> map = new HashMap<String, String>();
        map.put(e.getKey(), e.getValue());
        holdingBuffer.add(map);
      }
    }

    @Override
    public void deactivate()
    {
    }

    public void replayTuples(long windowId)
    {
    }
  }

  @Test
  public void testDag() throws Exception
  {
    String host = "localhost";
    int port = 1883;
    MqttClientConfig config = new MqttClientConfig();
    config.setHost(host);
    config.setPort(port);
    config.setCleanSession(true);
    sendingData.put("testa", "2");
    sendingData.put("testb", "20");
    sendingData.put("testc", "1000");
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    SourceModule source = dag.addOperator("source", SourceModule.class);

    TestMqttOutputOperator producer = dag.addOperator("producer", new TestMqttOutputOperator());
    producer.setMqttClientConfig(config);

    dag.addStream("Stream", source.outPort, producer.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    TestMqttOutputOperator.GetDataThread consumer = producer.new GetDataThread();
    producer.setup(null);

    consumer.start();

    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    try {
      Thread.sleep(2000);
    }
    catch (InterruptedException ex) {
    }
    lc.shutdown();

    junit.framework.Assert.assertEquals("emitted value for testNum was ", 3, receivedData.size());
    for (Map.Entry<String, String> e : receivedData.entrySet()) {
      if (e.getKey().equals("testa")) {
        junit.framework.Assert.assertEquals("emitted value for 'testa' was ", "2", e.getValue());
      }
      else if (e.getKey().equals("testb")) {
        junit.framework.Assert.assertEquals("emitted value for 'testb' was ", "20", e.getValue());
      }
      else if (e.getKey().equals("testc")) {
        junit.framework.Assert.assertEquals("emitted value for 'testc' was ", "1000", e.getValue());
      }
    }

    logger.debug("end of test");
  }

}
