/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.OperatorContext;
import com.malhartech.dag.TestSink;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.io.IOException;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class RabbitMQInputOperatorTest
{
  private static Logger logger = LoggerFactory.getLogger(RabbitMQInputOperatorTest.class);

  private static final class TestRabbitMQInputOperator extends AbstractRabbitMQInputOperator<String>
  {
    @Override
    public void emitTuple(byte[] message)
    {
      outputPort.emit(new String(message));
    }

//    @Override
//    public String getOutputTuple(byte[] message)
//    {
//      return new String(message);
//    }
    @Override
    public void emitTuples(long windowId)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void postActivate(OperatorContext ctx)
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
    final String exchange = "test";
    public String queueName;

    public void setup() throws IOException
    {
      connFactory.setHost("localhost");
      connection = connFactory.newConnection();
      channel = connection.createChannel();
//      channel.exchangeDeclare(exchange, "fanout");
    }

    public void process(Object message) throws IOException
    {
      String msg = message.toString();
//      logger.debug("publish:" + msg);
//      channel.basicPublish(exchange, "", null, msg.getBytes());
      channel.basicPublish("", queueName, null, msg.getBytes());
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

  @Test
  public void testProcess() throws Exception
  {
    final int testNum = 3;

    final TestRabbitMQInputOperator node = new TestRabbitMQInputOperator();
    TestSink<String> testSink = new TestSink<String>();
    node.outputPort.setSink(testSink);
    node.setHost("localhost");
    node.setExchange("test");
    node.setup(new OperatorConfiguration());


    final RabbitMQMessageGenerator publisher = new RabbitMQMessageGenerator();
    publisher.queueName = node.getQueueName();
    Thread generatorThread = new Thread()
    {
      @Override
      public void run()
      {
        try {
          publisher.setup();
          publisher.generateMessages(testNum);
        }
        catch (Exception ex) {
          logger.debug("generator exiting", ex);
        }
      }
    };
    generatorThread.start();

    testSink.waitForResultCount(testNum * 3, 2000);
    Assert.assertTrue("tuple emmitted", testSink.collectedTuples.size() > 0);

    Assert.assertEquals("emitted value for testNum was ", testNum * 3, testSink.collectedTuples.size());
    for (int i = 0; i < testSink.collectedTuples.size(); i++) {
      String str = testSink.collectedTuples.get(i);
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

    publisher.teardown();
    generatorThread.interrupt();
    node.preDeactivate();
    node.teardown();
    logger.debug("end of test sent " + testSink.collectedTuples.size() + " messages");
  }
}
