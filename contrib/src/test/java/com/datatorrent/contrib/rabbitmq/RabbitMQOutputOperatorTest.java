/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.rabbitmq;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.*;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.datatorrent.contrib.helper.SourceModule;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.LocalMode;

/**
 *
 */
public class RabbitMQOutputOperatorTest
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(RabbitMQOutputOperatorTest.class);

  public class RabbitMQMessageReceiver
  {
    public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    public int count = 0;
    private final String host = "localhost";
    ConnectionFactory connFactory = new ConnectionFactory();
    // QueueingConsumer consumer = null;
    Connection connection = null;
    Channel channel = null;
    TracingConsumer tracingConsumer = null;
    String cTag;
    String queueName = "testQ";
    String exchange = "testEx";

    public void setup() throws IOException
    {
      logger.debug("setting up receiver..");
      connFactory.setHost(host);
      connection = connFactory.newConnection();
      channel = connection.createChannel();

      channel.exchangeDeclare(exchange, "fanout");
      queueName = channel.queueDeclare().getQueue();
      channel.queueBind(queueName, exchange, "");

      tracingConsumer = new TracingConsumer(channel);
      cTag = channel.basicConsume(queueName, true, tracingConsumer);
    }

    public String getQueueName()
    {
      return queueName;
    }

    public void teardown() throws IOException
    {
      channel.close();
      connection.close();
    }

    public class TracingConsumer extends DefaultConsumer
    {
      public TracingConsumer(Channel ch)
      {
        super(ch);
      }

      @Override
      public void handleConsumeOk(String c)
      {
        logger.debug(this + ".handleConsumeOk(" + c + ")");
        super.handleConsumeOk(c);
      }

      @Override
      public void handleCancelOk(String c)
      {
        logger.debug(this + ".handleCancelOk(" + c + ")");
        super.handleCancelOk(c);
      }

      @Override
      public void handleShutdownSignal(String c, ShutdownSignalException sig)
      {
        logger.debug(this + ".handleShutdownSignal(" + c + ", " + sig + ")");
        super.handleShutdownSignal(c, sig);
      }

      @Override
      public void handleDelivery(String consumer_Tag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException
      {
        String str = new String(body);
        if (str.indexOf("{") == -1) {
          return;
        }
        int eq = str.indexOf('=');
        String key = str.substring(1, eq);
        int value = Integer.parseInt(str.substring(eq + 1, str.length() - 1));
        dataMap.put(key, value);
        count++;
      }
    }
  }

  @Test
  public void testDag() throws InterruptedException, MalformedURLException, IOException, Exception
  {
    final int testNum = 3;
    runTest(testNum);
    logger.debug("end of test");
  }

  protected void runTest(int testNum) throws IOException
  {
    RabbitMQMessageReceiver receiver = new RabbitMQMessageReceiver();
    receiver.setup();

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    SourceModule source = dag.addOperator("source", new SourceModule());
    source.setTestNum(testNum);
    RabbitMQOutputOperator collector = dag.addOperator("generator", new RabbitMQOutputOperator());
    collector.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());

    collector.setExchange("testEx");
    dag.addStream("Stream", source.outPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();
    try {
      Thread.sleep(1000);
      long timeout = 10000L;
      long startTms = System.currentTimeMillis();
      while ((receiver.count < testNum * 3) && (System.currentTimeMillis() - startTms < timeout)) {
        Thread.sleep(100);
      }
    } catch (InterruptedException ex) {
      Assert.fail(ex.getMessage());
    } finally {
      lc.shutdown();
    }

    Assert.assertEquals("emitted value for testNum was ", testNum * 3, receiver.count);
    for (Map.Entry<String, Integer> e : receiver.dataMap.entrySet()) {
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Integer(2), e.getValue());
      } else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted value for 'b' was ", new Integer(20), e.getValue());
      } else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted value for 'c' was ", new Integer(1000), e.getValue());
      }
    }
  }
}
