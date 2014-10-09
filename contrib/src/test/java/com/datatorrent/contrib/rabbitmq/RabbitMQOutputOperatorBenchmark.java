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
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import com.rabbitmq.client.*;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.ActivationListener;

/**
 *
 */
public class RabbitMQOutputOperatorBenchmark
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(RabbitMQOutputOperatorTest.class);

  private static final class TestRabbitMQOutputOperator extends AbstractSinglePortRabbitMQOutputOperator<String>
  {
    @Override
    public void processTuple(String tuple)
    {
      try {
        channel.basicPublish(exchange, "", null, tuple.getBytes());
//        channel.basicPublish("", queueName, null, tuple.getBytes());
      }
      catch (IOException ex) {
        logger.debug(ex.toString());
      }
    }
  }

  public class RabbitMQMessageReceiver
  {
    public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    public int count = 0;
    private final String host = "localhost";
    ConnectionFactory connFactory = new ConnectionFactory();
//  QueueingConsumer consumer = null;
    Connection connection = null;
    Channel channel = null;
    TracingConsumer tracingConsumer = null;
    String cTag;
    String queueName = "testQ";
    String exchange = "testEx";

    public void setup() throws IOException
    {
      connFactory.setHost(host);
      connection = connFactory.newConnection();
      channel = connection.createChannel();

      channel.exchangeDeclare(exchange, "fanout");
      queueName = channel.queueDeclare().getQueue();
      channel.queueBind(queueName, exchange, "");

//      consumer = new QueueingConsumer(channel);
//      channel.basicConsume(queueName, true, consumer);
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
//        logger.debug("Received Async message:" + new String(body));

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

  public static class SourceModule extends BaseOperator
          implements InputOperator, ActivationListener<OperatorContext>
  {
    public final transient DefaultOutputPort<String> outPort = new DefaultOutputPort<String>();
    transient ArrayBlockingQueue<byte[]> holdingBuffer;
    int testNum;

    @Override
    public void setup(OperatorContext context)
    {
      holdingBuffer = new ArrayBlockingQueue<byte[]>(1024 * 1024);
    }

    public void emitTuple(byte[] message)
    {
      outPort.emit(new String(message));
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
      for (int i = 0; i < testNum; i++) {
        HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
        dataMapa.put("a", 2);
        holdingBuffer.add(dataMapa.toString().getBytes());

        HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
        dataMapb.put("b", 20);
        holdingBuffer.add(dataMapb.toString().getBytes());

        HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
        dataMapc.put("c", 1000);
        holdingBuffer.add(dataMapc.toString().getBytes());
      }
    }

    public void setTestNum(int testNum)
    {
      this.testNum = testNum;
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
  public void testDag() throws InterruptedException, MalformedURLException, IOException, Exception
  {
    final int testNum = 100000;

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    final RabbitMQMessageReceiver receiver = new RabbitMQMessageReceiver();
    receiver.setup();

    SourceModule source = dag.addOperator("source", SourceModule.class);
    source.setTestNum(testNum);
    TestRabbitMQOutputOperator collector = dag.addOperator("generator", new TestRabbitMQOutputOperator());
//    collector.setQueueName("testQ");
    collector.setExchange("testEx");
    dag.addStream("Stream", source.outPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);


    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    try {
      Thread.sleep(1000);
      while (true) {
        if (receiver.count < testNum * 3) {
          Thread.sleep(10);
        }
        else {
          break;
        }
      }
    }
    catch (InterruptedException ex) {
    }
    lc.shutdown();

    Assert.assertEquals("emitted value for testNum was ", testNum * 3, receiver.count);
    for (Map.Entry<String, Integer> e : receiver.dataMap.entrySet()) {
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Integer(2), e.getValue());
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted value for 'b' was ", new Integer(20), e.getValue());
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted value for 'c' was ", new Integer(1000), e.getValue());
      }
    }
    logger.debug(String.format("\nBenchmarked %d tuples", testNum * 3));
    logger.debug("end of test");  }
}
