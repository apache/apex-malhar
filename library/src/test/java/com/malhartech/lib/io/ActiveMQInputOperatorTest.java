/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.*;
import com.malhartech.dag.TestSink;
import com.malhartech.stram.StramLocalCluster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import junit.framework.Assert;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class ActiveMQInputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQInputOperatorTest.class);
  private static BrokerService broker;
  static int debugMessageCount = 5;  // for debug, display first few messages
  public static HashMap<Integer, String> receivedData = new HashMap<Integer, String>();
  private static int receivedCount = 0;
  static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();

  /**
   * An example Concrete class of ActiveMQInputOperator for testing.
   */
  public static class ActiveMQInputOperator extends AbstractActiveMQInputOperator<String>
  {
    @OutputPortFieldAnnotation(name = "outputPort")
    public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>(this);

    @Override
    protected String getTuple(Message message) throws JMSException
    {
      // outputPort.emit(message); should fail
      //logger.debug("getTuple() got called from {}", this);
      if (message instanceof TextMessage) {
        String msg = ((TextMessage)message).getText();
        logger.debug("Received Message: {}", msg);
        receivedData.put(new Integer(++receivedCount), msg);
        return msg;
      }
      else {
        throw new IllegalArgumentException("Unhandled message type " + message.getClass().getName());
      }
    }

    @Override
    protected void emitTuple(String t) throws JMSException
    {
       //super.outputPort.emit(t);
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void run()
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void replayTuples(long windowId)
    {
      int count = 0;
      while (tuples.hasNext()) {
        //outputPort.emit(tuples.next());

    @Override
    public void emitTuples(long windowId)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    startActiveMQService();
  }

  /**
   * Start ActiveMQ broker from the Testcase.
   *
   * @throws Exception
   */
  private static void startActiveMQService() throws Exception
  {
    broker = new BrokerService();
    broker.addConnector("tcp://localhost:61617?broker.persistent=false");
    broker.getSystemUsage().getStoreUsage().setLimit(1024 * 1024 * 1024);  // 1GB
    broker.getSystemUsage().getTempUsage().setLimit(100 * 1024 * 1024);    // 100MB
    broker.setDeleteAllMessagesOnStartup(true);
    broker.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    broker.stop();
  }

  public class CollectorInputPort<T> extends DefaultInputPort<T>
  {
    ArrayList<T> list;
    final String id;

    public CollectorInputPort(String id, Operator module)
    {
      super(module);
      this.id = id;
    }

    @Override
    public void process(T tuple)
    {
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

  public class CollectorModule<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("myInput", this);
  }

  /**
   * Test AbstractActiveMQInputOperator (i.e. an input adapter for ActiveMQ, aka consumer).
   * This module receives data from an outside test generator through ActiveMQ message bus and
   * feed that data into Malhar streaming platform.
   *
   * [Generate message and send that to ActiveMQ message bus] ==>
   * [Receive that message through ActiveMQ input adapter(i.e. consumer) and send via emit() in output port during onMessage call]
   *
   * Note: We don't need to test the data is being send to sink through output port.
   *
   * @throws Exception
   */
  // @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testActiveMQInputOperator() throws Exception
  {
    // Setup a message generator to receive the message from.
    ActiveMQMessageGenerator generator = new ActiveMQMessageGenerator();
    try {
      generator.setDebugMessageCount(debugMessageCount);
      generator.setupConnection();
      generator.sendMessage();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
    generator.closeConnection();

    // The output port of node should be connected to a Test sink.
    TestSink<String> outSink = new TestSink<String>();
    ActiveMQInputOperator node = new ActiveMQInputOperator();
    ActiveMQConsumerBase config = node.getAmqConsumer();
    config.setUser("");
    config.setPassword("");
    config.setUrl("tcp://localhost:61617");
    config.setAckMode("CLIENT_ACKNOWLEDGE");
    config.setClientId("Client1");
    config.setConsumerName("Consumer1");
    config.setSubject("TEST.FOO");
    config.setMaximumMessage(20);
    config.setMessageSize(255);
    config.setBatch(10);
    config.setTopic(false);
    config.setDurable(false);
    config.setTransacted(false);
    config.setVerbose(true);


    //  node.outputPort.setSink(outSink);
    node.setup(new OperatorConfiguration());

    // Allow some time to receive data.
    Thread.sleep(2000);

    // Check values send vs received.
    int totalCount = receivedData.size();
    Assert.assertEquals("Number of emitted tuples", generator.sendCount, totalCount);
    logger.debug(String.format("Number of emitted tuples: %d", totalCount));

    // Check contents only for first few.
    for (int i = 1; i <= debugMessageCount & i < totalCount; ++i) {
      Assert.assertEquals("Message content", generator.sendData.get(i), receivedData.get(new Integer(i)));
      logger.debug(String.format("Received: %s", receivedData.get(new Integer(i))));
    }
    receivedData.clear();
    config.cleanup();

  }

  @Test
  public void test2() throws Exception
  {
    // Setup a message generator to receive the message from.
    ActiveMQMessageGenerator generator = new ActiveMQMessageGenerator();
    try {
      generator.setDebugMessageCount(debugMessageCount);
      generator.setupConnection();
      generator.sendMessage();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
    generator.closeConnection();




    DAG dag = new DAG();
    ActiveMQInputOperator node = dag.addOperator("AMQ message consumer", ActiveMQInputOperator.class);
    ActiveMQConsumerBase config = node.getAmqConsumer();
    config.setUser("");
    config.setPassword("");
    config.setUrl("tcp://localhost:61617");
    config.setAckMode("CLIENT_ACKNOWLEDGE");
    config.setClientId("Client1");
    config.setConsumerName("Consumer1");
    config.setSubject("TEST.FOO");
    config.setMaximumMessage(7);
    config.setMessageSize(255);
    config.setBatch(10);
    config.setTopic(false);
    config.setDurable(false);
    config.setTransacted(true); // this flag is different than prior test
    config.setVerbose(true);

    CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    dag.addStream("AMQ message", node.outputPort, collector.inputPort).setInline(true);

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

    Assert.assertEquals("Collections size", 1, collections.size());
    Assert.assertEquals("Tuple count", 10, collections.get(collector.inputPort.id).size());
    config.cleanup();
  }
}
