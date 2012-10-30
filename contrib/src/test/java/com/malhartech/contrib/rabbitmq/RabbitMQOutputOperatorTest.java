/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.rabbitmq;

import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.*;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class RabbitMQOutputOperatorTest
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(RabbitMQOutputOperatorTest.class);

  private static final class TestRabbitMQOutputOperator extends AbstractRabbitMQOutputOperator<String>
  {
    private int testNum;
    public final transient DefaultInputPort<String> input = new DefaultInputPort<String>(this)
    {
      @Override
      public void process(String message)
      {
        try {
          channel.basicPublish("", queueName, null, message.getBytes());
        }
        catch (IOException ex) {
          logger.debug(ex.toString());
        }
      }
    };

    public void setTestNum(int testNum)
    {
      this.testNum = testNum;
    }

    public void generateMessages()
    {
      Sink testSink = input.getSink();
      beginWindow();
      for (int i = 0; i < testNum; i++) {
        HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
        dataMapa.put("a", 2);
        testSink.process(dataMapa.toString());

        HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
        dataMapb.put("b", 20);
        testSink.process(dataMapb.toString());

        HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
        dataMapc.put("c", 1000);
        testSink.process(dataMapc.toString());
      }
      endWindow();
      teardown();
    }
  }

  public class RabbitMQMessageReceiver
  {
    public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    public int count = 0;
    private String host = "localhost";
    ConnectionFactory connFactory = new ConnectionFactory();
//  QueueingConsumer consumer = null;
    Connection connection = null;
    Channel channel = null;
    TracingConsumer tracingConsumer = null;
    String cTag;
    String queueName;

    public void setup() throws IOException
    {
      connFactory.setHost(host);
      connection = connFactory.newConnection();
      channel = connection.createChannel();

//      channel.exchangeDeclare(exchange, "fanout");
      queueName = channel.queueDeclare().getQueue();

//      channel.queueBind(queueName, exchange, "");
//      consumer = new QueueingConsumer(channel);
//      channel.basicConsume(queueName, true, consumer);
      tracingConsumer = new TracingConsumer(channel);
      cTag = channel.basicConsume(queueName, true, tracingConsumer);
    }

    public String getQueueName() {
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
        logger.debug("Received Async message:" + new String(body));
        // convert to HashMap and save the values for each key
        // then expect c to be 1000, b=20, a=2
        // and do count++ (where count now would be 30)
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
  public void testProcess() throws InterruptedException, MalformedURLException, IOException
  {
    final int testNum = 3;

    RabbitMQMessageReceiver receiver = new RabbitMQMessageReceiver();
    receiver.setup();

    TestRabbitMQOutputOperator node = new TestRabbitMQOutputOperator();
    node.setup(new OperatorConfiguration());
    node.setQueueName(receiver.getQueueName());
    node.setTestNum(testNum);
    node.generateMessages();

    while (receiver.count < testNum * 3) {
      Thread.sleep(1);
    }
    junit.framework.Assert.assertEquals("emitted value for testNum was ", testNum * 3, receiver.count);
    for (Map.Entry<String, Integer> e: receiver.dataMap.entrySet()) {
      if (e.getKey().equals("a")) {
        junit.framework.Assert.assertEquals("emitted value for 'a' was ", new Integer(2), e.getValue());
      }
      else if (e.getKey().equals("b")) {
        junit.framework.Assert.assertEquals("emitted value for 'b' was ", new Integer(20), e.getValue());
      }
      else if (e.getKey().equals("c")) {
        junit.framework.Assert.assertEquals("emitted value for 'c' was ", new Integer(1000), e.getValue());
      }
    }
//    subThr.interrupt();
    logger.debug("end of test");
  }
}
