/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.rabbitmq;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.*;
import com.malhartech.stram.StramLocalCluster;
import com.malhartech.util.CircularBuffer;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
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
    public final transient DefaultInputPort<String> inputPort = new DefaultInputPort<String>(this)
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
    String queueName = "testQ";

    public void setup() throws IOException
    {
      connFactory.setHost(host);
      connection = connFactory.newConnection();
      channel = connection.createChannel();

//      channel.exchangeDeclare(exchange, "fanout");

//      channel.queueBind(queueName, exchange, "");
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
        logger.debug("Received Async message:" + new String(body));

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

  /**
   *
   * @param <T>
   */
  public static class SourceOutputPort<T> extends DefaultOutputPort<T>
  {
    SourceOutputPort(Operator op)
    {
      super(op);
    }
  }

  public static class SourceModule extends BaseOperator
          implements InputOperator, ActivationListener<OperatorContext>
  {
    public final transient SourceOutputPort<String> outPort = new SourceOutputPort<String>(this);
    transient CircularBuffer<byte[]> holdingBuffer;
    int testNum;

    @Override
    public void setup(OperatorContext context)
    {
      holdingBuffer = new CircularBuffer<byte[]>(1024 * 1024);
    }

    public void emitTuple(byte[] message)
    {
      outPort.emit(new String(message));
    }

    @Override
    public void emitTuples()
    {
      for (int i = holdingBuffer.size(); i-- > 0;) {
        emitTuple(holdingBuffer.pollUnsafe());
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

    public void deactivate()
    {
    }

    public void replayTuples(long windowId)
    {
    }
  }

  @Test
  public void testProcess() throws InterruptedException, MalformedURLException, IOException, Exception
  {
    final int testNum = 3;

    DAG dag = new DAG();
    SourceModule source = dag.addOperator("source", SourceModule.class);
    source.setTestNum(testNum);
    TestRabbitMQOutputOperator collector = dag.addOperator("generator", new TestRabbitMQOutputOperator());
    collector.setQueueName("testQ");
    dag.addStream("Stream", source.outPort, collector.inputPort).setInline(true);

    RabbitMQMessageReceiver receiver = new RabbitMQMessageReceiver();
    receiver.setup();

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException ex) {
        }
        lc.shutdown();
      }
    }.start();

    lc.run();

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
    logger.debug("end of test");
  }
}
