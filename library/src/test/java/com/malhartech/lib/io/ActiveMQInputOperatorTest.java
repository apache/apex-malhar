/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import com.malhartech.stram.StramLocalCluster;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import junit.framework.Assert;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
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
  private BrokerService broker;
  static int debugMessageCount = 5;  // for debug, display first few messages
  public static HashMap<Integer, String> receivedData = new HashMap<Integer, String>();
  private static int receivedCount = 0;
  static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();

  /**
   * An example Concrete class of ActiveMQStringSinglePortInputOperator for testing.
   */
  public static class ActiveMQStringSinglePortInputOperator extends AbstractActiveMQSinglePortInputOperator<String>
  {
   // @OutputPortFieldAnnotation(name = "outputPort")
  //  public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>(this);

    /**
     * Implement abstract method of AbstractActiveMQInputOperator
     */
    @Override
    protected String getTuple(Message message) throws JMSException
    {
      //logger.debug("getTuple got called from {}", this);
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

    /**
     * Implement InputOperator Interface.
     */
  /*  @Override
    public void emitTuples()
    {
      //logger.debug("emitTuples got called from {}", this);
      int bufferSize = holdingBuffer.size();
      for (int i = getTuplesBlast() < bufferSize ? getTuplesBlast() : bufferSize; i-- > 0;) {
        String tuple = holdingBuffer.pollUnsafe();
        outputPort.emit(tuple);
        logger.debug("emitTuples() got called from {} with tuple: {}", this, tuple);
      }
    }*/
  } // End of ActiveMQStringSinglePortInputOperator

  /**
   * Start ActiveMQ broker from the Testcase.
   *
   * @throws Exception
   */
  private void startActiveMQService() throws Exception
  {
    broker = new BrokerService();
    String brokerName = "ActiveMQInputOperator-broker";
    broker.setBrokerName(brokerName);
    broker.getPersistenceAdapter().setDirectory(new File("target/test-data/activemq-data/" + broker.getBrokerName() + "/KahaDB"));
    broker.addConnector("tcp://localhost:61617?broker.persistent=false");
    broker.getSystemUsage().getStoreUsage().setLimit(1024 * 1024 * 1024);  // 1GB
    broker.getSystemUsage().getTempUsage().setLimit(100 * 1024 * 1024);    // 100MB
    broker.setDeleteAllMessagesOnStartup(true);
    broker.start();
  }

  @Before
  public void beforTest() throws Exception
  {
    startActiveMQService();
  }

  @After
  public void afterTest() throws Exception
  {
    broker.stop();
    try {
      Thread.sleep(1000);
    }
    catch (InterruptedException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  /**
   * Test Operator to collect tuples from ActiveMQStringSinglePortInputOperator.
   *
   * @param <T>
   */
  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("myInput", this);
  }

  public static class CollectorInputPort<T> extends DefaultInputPort<T>
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

  /**
   * Test AbstractActiveMQInputOperator (i.e. an input adapter for ActiveMQ, aka consumer).
   * This module receives data from an outside test generator through ActiveMQ message bus and
   * feed that data into Malhar streaming platform.
   *
   * [Generate message and send that to ActiveMQ message bus] ==>
   * [Receive that message through ActiveMQ input adapter(i.e. consumer) and send using emitTuples() interface on output port during onMessage call]
   *
   *
   * @throws Exception
   */
//@Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testActiveMQInputOperatorWithoutUsingDAG() throws Exception
  {
    /*   // Setup a message generator to receive the message from.
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
     ActiveMQStringSinglePortInputOperator node = new ActiveMQStringSinglePortInputOperator();
     ActiveMQConsumerBase config = node.getAmqConsumer();
     config.setUser("");
     config.setPassword("");
     config.setUrl("tcp://localhost:61617");
     config.setAckMode("CLIENT_ACKNOWLEDGE");
     config.setClientId("Client1");
     config.setConsumerName("Consumer1");
     config.setSubject("TEST.FOO");
     config.setMaximumReceiveMessages(20);
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
     config.cleanup(); */
  }

  /**
   * Test AbstractActiveMQInputOperator (i.e. an input adapter for ActiveMQ, aka consumer).
   * This module receives data from an outside test generator through ActiveMQ message bus and
   * feed that data into Malhar streaming platform.
   *
   * [Generate message and send that to ActiveMQ message bus] ==>
   * [Receive that message through ActiveMQ input adapter(i.e. consumer) and send using emitTuples() interface on output port during onMessage call]
   *
   *
   * @throws Exception
   */
  @Test
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

    // Create DAG for testing.
    DAG dag = new DAG();
    // Create ActiveMQStringSinglePortInputOperator
    ActiveMQStringSinglePortInputOperator node = dag.addOperator("AMQ message consumer", ActiveMQStringSinglePortInputOperator.class);
    // Set configuration parameters for ActiveMQ
    node.setUser("");
    node.setPassword("");
    node.setUrl("tcp://localhost:61617");
    node.setAckMode("CLIENT_ACKNOWLEDGE");
    node.setClientId("Client1");
    node.setConsumerName("Consumer1");
    node.setSubject("TEST.FOO");
    node.setMaximumReceiveMessages(7);
    node.setMessageSize(255);
    node.setBatch(10);
    node.setTopic(false);
    node.setDurable(false);
    node.setTransacted(true); // this flag is different than prior test
    node.setVerbose(true);

    // Create Test tuple collector
    CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("AMQ message", node.outputPort, collector.inputPort).setInline(true);

    // Create local cluster
    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    // Run local cluster
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

    // Check results
    Assert.assertEquals("Collections size", 1, collections.size());
    Assert.assertEquals("Tuple count", 7, collections.get(collector.inputPort.id).size());
  }

  @Test
  public void testActiveMQInputOperator2() throws Exception
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

    // Create DAG for testing.
    DAG dag = new DAG();
    // Create ActiveMQStringSinglePortInputOperator
    ActiveMQStringSinglePortInputOperator node = dag.addOperator("AMQ message consumer", ActiveMQStringSinglePortInputOperator.class);
    // Set configuration parameters for ActiveMQ
    node.setUser("");
    node.setPassword("");
    node.setUrl("tcp://localhost:61617");
    node.setAckMode("CLIENT_ACKNOWLEDGE");
    node.setClientId("Client1");
    node.setConsumerName("Consumer1");
    node.setSubject("TEST.FOO");
    node.setMaximumReceiveMessages(0);
    node.setMessageSize(255);
    node.setBatch(10);
    node.setTopic(false);
    node.setDurable(false);
    node.setTransacted(true); // this flag is different than prior test
    node.setVerbose(true);

    // Create Test tuple collector
    CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("AMQ message", node.outputPort, collector.inputPort).setInline(true);

    // Create local cluster
    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    // Run local cluster
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

    // Check results
    Assert.assertEquals("Collections size", 1, collections.size());
    Assert.assertEquals("Tuple count", 20, collections.get(collector.inputPort.id).size());
  }
  }
