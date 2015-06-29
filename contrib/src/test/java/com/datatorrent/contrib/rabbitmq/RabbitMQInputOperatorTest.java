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

import com.datatorrent.contrib.helper.CollectorModule;
import com.datatorrent.contrib.helper.MessageQueueTestHelper;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.LocalMode;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.netlet.util.DTThrowable;

/**
 *
 */
public class RabbitMQInputOperatorTest
{
  private static Logger logger = LoggerFactory.getLogger(RabbitMQInputOperatorTest.class);

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
    }

    public void setQueueName(String queueName)
    {
      this.queueName = queueName;
    }

    public void process(Object message) throws IOException
    {
      String msg = message.toString();
      channel.basicPublish(exchange, "", null, msg.getBytes());
    }

    public void teardown() throws IOException
    {
      channel.close();
      connection.close();
    }

    public void generateMessages(int msgCount) throws InterruptedException, IOException
    {
      for (int i = 0; i < msgCount; i++) {

        ArrayList<HashMap<String, Integer>> dataMaps = MessageQueueTestHelper.getMessages();
        for (int j = 0; j < dataMaps.size(); j++) {
          process(dataMaps.get(j));
        }
      }
    }

  }

  @Test
  public void testDag() throws Exception
  {
    final int testNum = 3;
    runTest(testNum);
    logger.debug("end of test");
  }

  protected void runTest(final int testNum) throws IOException
  {
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    RabbitMQInputOperator consumer = dag.addOperator("Consumer", RabbitMQInputOperator.class);
    consumer.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());

    final CollectorModule<byte[]> collector = dag.addOperator("Collector", new CollectorModule<byte[]>());

    consumer.setHost("localhost");
    consumer.setExchange("testEx");
    consumer.setExchangeType("fanout");

    final RabbitMQMessageGenerator publisher = new RabbitMQMessageGenerator();
    publisher.setup();

    dag.addStream("Stream", consumer.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        long startTms = System.currentTimeMillis();
        long timeout = 100000L;
        try {
          while (!collector.inputPort.collections.containsKey("collector") && System.currentTimeMillis() - startTms < timeout) {
            Thread.sleep(500);
          }
          publisher.generateMessages(testNum);
          startTms = System.currentTimeMillis();
          while (System.currentTimeMillis() - startTms < timeout) {
            List<?> list = collector.inputPort.collections.get("collector");

            if (list.size() < testNum * 3) {
              Thread.sleep(10);
            } else {
              break;
            }
          }
        } catch (IOException ex) {
          logger.error(ex.getMessage(), ex);
          DTThrowable.rethrow(ex);
        } catch (InterruptedException ex) {
          DTThrowable.rethrow(ex);
        } finally {
          lc.shutdown();
        }
      }

    }.start();

    lc.run();

    logger.debug("collection size: {} {}", collector.inputPort.collections.size(), collector.inputPort.collections);

    MessageQueueTestHelper.validateResults(testNum, collector.inputPort.collections);
  }

  @Test
  public void testRecoveryAndIdempotency() throws Exception
  {
    RabbitMQInputOperator operator = new RabbitMQInputOperator();
    operator.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());
    operator.setHost("localhost");
    operator.setExchange("testEx");
    operator.setExchangeType("fanout");

    Attribute.AttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();

    operator.outputPort.setSink(sink);
    OperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributeMap);

    operator.setup(context);
    operator.activate(context);

    final RabbitMQMessageGenerator publisher = new RabbitMQMessageGenerator();
    publisher.setup();
    publisher.generateMessages(5);

    Thread.sleep(10000);

    operator.beginWindow(1);
    operator.emitTuples();
    operator.endWindow();

    operator.deactivate();
    Assert.assertEquals("num of messages in window 1", 15, sink.collectedTuples.size());

    // failure and then re-deployment of operator
    sink.collectedTuples.clear();
    operator.setup(context);
    operator.activate(context);

    Assert.assertEquals("largest recovery window", 1, operator.getIdempotentStorageManager().getLargestRecoveryWindow());
    operator.beginWindow(1);
    operator.endWindow();
    Assert.assertEquals("num of messages in window 1", 15, sink.collectedTuples.size());
    sink.collectedTuples.clear();

    operator.deactivate();
    operator.teardown();
    operator.getIdempotentStorageManager().deleteUpTo(context.getId(), 1);
    publisher.teardown();
  }
}
