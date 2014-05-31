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
package com.datatorrent.contrib.rabbitmq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.*;
import com.datatorrent.api.DAG.Locality;

/**
 *
 */
public class RabbitMQInputOperatorTest
{
  private static Logger logger = LoggerFactory.getLogger(RabbitMQInputOperatorTest.class);
  static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();

  public static final class TestStringRabbitMQInputOperator extends AbstractSinglePortRabbitMQInputOperator<String>
  {
    @Override
    public String getTuple(byte[] message)
    {
      return new String(message);
    }

    public void replayTuples(long windowId)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

  }

  private final class RabbitMQMessageGenerator
  {
    ConnectionFactory connFactory = new ConnectionFactory();
    QueueingConsumer consumer = null;
    Connection connection = null;
    Channel channel = null;
    final String exchange = "testEx";
    public String queueName = "testQ";

    public void setup() throws IOException
    {
      connFactory.setHost("localhost");
      connection = connFactory.newConnection();
      channel = connection.createChannel();
      channel.exchangeDeclare(exchange, "fanout");
//      channel.queueDeclare(queueName, false, false, false, null);
    }

    public void setQueueName(String queueName)
    {
      this.queueName = queueName;
    }

    public void process(Object message) throws IOException
    {
      String msg = message.toString();
//      logger.debug("publish:" + msg);
      channel.basicPublish(exchange, "", null, msg.getBytes());
//      channel.basicPublish("", queueName, null, msg.getBytes());
    }

    public void teardown() throws IOException
    {
      channel.close();
      connection.close();
    }

    public void generateMessages(int msgCount) throws InterruptedException, IOException
    {
      for (int i = 0; i < msgCount; i++) {
        HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
        dataMapa.put("a", 2);
        process(dataMapa);

        HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
        dataMapb.put("b", 20);
        process(dataMapb);

        HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
        dataMapc.put("c", 1000);
        process(dataMapc);
      }
    }

  }

  public static class CollectorInputPort<T> extends DefaultInputPort<T>
  {
    ArrayList<T> list;
    final String id;

    public CollectorInputPort(String id, Operator module)
    {
      super();
      this.id = id;
    }

    @Override
    public void process(T tuple)
    {
//      System.out.print("collector process:"+tuple);
      list.add(tuple);
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        collections.put(id, list = new ArrayList<T>());
      }
    }

  }

  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("collector", this);
  }

  @Test
  public void testDag() throws Exception
  {
    final int testNum = 3;
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    TestStringRabbitMQInputOperator consumer = dag.addOperator("Consumer", TestStringRabbitMQInputOperator.class);
    CollectorModule<String> collector = dag.addOperator("Collector", new CollectorModule<String>());

    consumer.setHost("localhost");
    consumer.setExchange("testEx");
    consumer.setExchangeType("fanout");

    final RabbitMQMessageGenerator publisher = new RabbitMQMessageGenerator();
    publisher.setup();
//    publisher.generateMessages(testNum);

    dag.addStream("Stream", consumer.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        long startTms = System.currentTimeMillis();
        long timeout = 10000L;
        try {
          while (!collections.containsKey("collector") && System.currentTimeMillis() - startTms < timeout) {
            Thread.sleep(500);
          }
          publisher.generateMessages(testNum);
          while (System.currentTimeMillis() - startTms < timeout) {
            @SuppressWarnings("unchecked")
            ArrayList<String> strList = (ArrayList<String>)collections.get("collector");
            if (strList == null || strList.size() < testNum * 3) {
              Thread.sleep(10);
            }
            else {
              break;
            }
          }
        }
        catch (IOException ex) {
          logger.debug(ex.toString());
        }
        catch (InterruptedException ex) {
        }
        lc.shutdown();
      }

    }.start();

    lc.run();

    logger.debug("collection size:" + collections.size() + " " + collections.toString());

    ArrayList<String> strList = (ArrayList<String>)collections.get("collector");
    Assert.assertNotNull("collector list", strList);
    Assert.assertEquals("emitted value for testNum was ", testNum * 3, strList.size());
    for (int i = 0; i < strList.size(); i++) {
      String str = strList.get(i);
      int eq = str.indexOf('=');
      String key = str.substring(1, eq);
      Integer value = Integer.parseInt(str.substring(eq + 1, str.length() - 1));
      if (key.equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Integer(2), value);
      }
      else if (key.equals("b")) {
        Assert.assertEquals("emitted value for 'b' was ", new Integer(20), value);
      }
      if (key.equals("c")) {
        Assert.assertEquals("emitted value for 'c' was ", new Integer(1000), value);
      }
    }
    logger.debug("end of test");
  }

}
