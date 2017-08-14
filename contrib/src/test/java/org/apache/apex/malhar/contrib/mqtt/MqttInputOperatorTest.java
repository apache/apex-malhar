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
package org.apache.apex.malhar.contrib.mqtt;

import java.util.HashMap;
import java.util.Map.Entry;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;

public class MqttInputOperatorTest
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MqttInputOperatorTest.class);
  private static HashMap<String, Object> resultMap = new HashMap<String, Object>();
  private static int resultCount = 0;
  private static boolean activated = false;

  public static class TestMqttInputOperator extends AbstractSinglePortMqttInputOperator<KeyValPair<String, String>>
  {
    @Override
    public KeyValPair<String, String> getTuple(Message msg)
    {
      return new KeyValPair<String, String>(msg.getTopic(), new String(msg.getPayload()));
    }

    public void generateData() throws Exception
    {
      HashMap<String, Integer> map = new HashMap<String, Integer>();
      map.put("a", 10);
      map.put("b", 200);
      map.put("c", 3000);
      for (Entry<String, Integer> entry : map.entrySet()) {
        connection.publish(entry.getKey(), entry.getValue().toString().getBytes(), QoS.AT_MOST_ONCE, false);
      }
    }

    @Override
    public void activate(OperatorContext context)
    {
      super.activate(context);
      activated = true;
    }

  }

  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
    {
      @Override
      public void process(T t)
      {
        @SuppressWarnings("unchecked")
        KeyValPair<String, String> kvp = (KeyValPair<String, String>)t;
        resultMap.put(kvp.getKey(), kvp.getValue());
        resultCount++;
      }

    };
  }

  @Test
  public void testInputOperator() throws InterruptedException, Exception
  {
    String host = "localhost";
    int port = 1883;
    MqttClientConfig config = new MqttClientConfig();
    config.setHost(host);
    config.setPort(port);
    config.setCleanSession(true);

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    final TestMqttInputOperator input = dag.addOperator("input", TestMqttInputOperator.class);
    CollectorModule<KeyValPair<String, String>> collector = dag.addOperator("collector", new CollectorModule<KeyValPair<String, String>>());

    input.addSubscribeTopic("a", QoS.AT_MOST_ONCE);
    input.addSubscribeTopic("b", QoS.AT_MOST_ONCE);
    input.addSubscribeTopic("c", QoS.AT_MOST_ONCE);
    input.setMqttClientConfig(config);
    input.setup(null);
    dag.addStream("stream", input.outputPort, collector.inputPort);

    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    long timeout = System.currentTimeMillis() + 3000;
    while (true) {
      if (activated) {
        break;
      }
      Assert.assertTrue("Activation timeout", timeout > System.currentTimeMillis());
      Thread.sleep(1000);
    }

    input.activate(null);
    //Thread.sleep(3000);
    input.generateData();

    long timeout1 = System.currentTimeMillis() + 5000;
    try {
      while (true) {
        if (resultCount == 0) {
          Thread.sleep(10);
          Assert.assertTrue("timeout without getting any data", System.currentTimeMillis() < timeout1);
        } else {
          break;
        }
      }
    } catch (InterruptedException ex) {
      // ignore
    }
    lc.shutdown();

    Assert.assertEquals("Number of emitted tuples", 3, resultMap.size());
    Assert.assertEquals("value of a is ", "10", resultMap.get("a"));
    Assert.assertEquals("value of b is ", "200", resultMap.get("b"));
    Assert.assertEquals("value of c is ", "3000", resultMap.get("c"));
    logger.debug("resultCount:" + resultCount);
  }

}
