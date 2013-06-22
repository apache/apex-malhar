/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.rabbitmq;

import com.datatorrent.contrib.rabbitmq.AbstractSinglePortRabbitMQInputOperator;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class RabbitMQInputOperatorBenchmark
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

    public void send(Object message) throws IOException
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
        send(dataMapa);

        HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
        dataMapb.put("b", 20);
        send(dataMapb);

        HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
        dataMapc.put("c", 1000);
        send(dataMapc);
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
//      System.out.print("collector process:" + tuple);
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
    final int testNum = 100000;
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    TestStringRabbitMQInputOperator subscriber = dag.addOperator("Generator", TestStringRabbitMQInputOperator.class);
    CollectorModule<String> collector = dag.addOperator("Collector", new CollectorModule<String>());

    subscriber.setHost("localhost");
    subscriber.setExchange("testEx");

    final RabbitMQMessageGenerator publisher = new RabbitMQMessageGenerator();
    publisher.setup();
//    publisher.generateMessages(testNum);

    dag.addStream("Stream", subscriber.outputPort, collector.inputPort).setInline(true);

    final LocalMode.Controller lc = lma.getController();

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(500);
          publisher.generateMessages(testNum);
          while (true) {
            ArrayList<String> strList = (ArrayList<String>)collections.get("collector");
            if (strList.size() < testNum * 3) {
              Thread.sleep(10);
            }
            else {
              break;
            }
          }
        }
        catch (InterruptedException ex) {
          logger.debug(ex.toString());
        }
        catch (IOException ex) {
          logger.debug(ex.toString());
        }
        lc.shutdown();
      }
    }.start();

    lc.run();

//    logger.debug("collection size:" + collections.size() + " " + collections.toString());

    ArrayList<String> strList = (ArrayList<String>)collections.get("collector");
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
    logger.debug(String.format("\nBenchmarked %d tuples", testNum * 3));
    logger.debug("end of test");
  }
}
