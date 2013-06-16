/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io;

import com.datatorrent.lib.io.AbstractActiveMQInputOperator;
import com.datatorrent.lib.io.AbstractActiveMQSinglePortInputOperator;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.*;
import com.malhartech.stram.StramLocalCluster;
import com.malhartech.stram.plan.logical.LogicalPlan;
import com.malhartech.stram.support.StramTestSupport;
import com.malhartech.stram.support.StramTestSupport.WaitCondition;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.StreamMessage;
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
 *
 */
public class ActiveMQInputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQInputOperatorTest.class);
  private BrokerService broker;
  static int debugMessageCount = 5;  // for debug, display first few messages
  static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();

  /**
   * An example Concrete class of ActiveMQSinglePortInputOperator for testing.
   */
  public static class ActiveMQStringSinglePortInputOperator extends AbstractActiveMQSinglePortInputOperator<String>
  {
    /**
     * Implement abstract method of AbstractActiveMQSinglePortInputOperator
     */
    @Override
    public String getTuple(Message message)
    {
      String msg = null;
      try {
        if (message instanceof TextMessage) {
          msg = ((TextMessage)message).getText();
          //logger.debug("Received Message: {}", msg);
        }
        else if (message instanceof StreamMessage) {
          msg = ((StreamMessage)message).readString();
        }
        else {
          throw new IllegalArgumentException("Unhandled message type " + message.getClass().getName());
        }
      }
      catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
        return msg;
      }
      return msg;
    }
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
  public void beforeTest() throws Exception
  {
    startActiveMQService();
  }

  @After
  public void afterTest() throws Exception
  {
    broker.stop();
    collections.clear();
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

  /**
   * Test Operator to collect tuples from ActiveMQMultiPortInputOperator.
   *
   * @param <T1, T2>
   */
  public static class CollectorModule2<T1, T2> extends BaseOperator
  {
    public final transient CollectorInputPort<T1> inputPort1 = new CollectorInputPort<T1>("myInput1", this);
    public final transient CollectorInputPort<T2> inputPort2 = new CollectorInputPort<T2>("myInput2", this);
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
   * Test AbstractActiveMQSinglePortInputOperator (i.e. an input adapter for ActiveMQ, aka consumer).
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
    LogicalPlan dag = new LogicalPlan();
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
    final CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("AMQ message", node.outputPort, collector.inputPort).setInline(true);

    // Create and run local cluster
    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();
    WaitCondition c = new WaitCondition() {
      @Override
      public boolean isComplete() {
        if ( collections.size() > 0) {
          return collections.get(collector.inputPort.id).size() >= 7;
        }
        else {
          return false;
        }
      }
    };
    StramTestSupport.awaitCompletion(c, 10000);
    lc.shutdown();

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
    LogicalPlan dag = new LogicalPlan();
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
    final CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("AMQ message", node.outputPort, collector.inputPort).setInline(true);

    // Create and run local cluster
    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();
    WaitCondition c = new WaitCondition() {
      @Override
      public boolean isComplete() {
        if ( collections.size() > 0) {
          return collections.get(collector.inputPort.id).size() >= 20;
        }
        else {
          return false;
        }
      }
    };
    StramTestSupport.awaitCompletion(c, 10000);
    lc.shutdown();

    // Check results
    Assert.assertEquals("Collections size", 1, collections.size());
    Assert.assertEquals("Tuple count", 20, collections.get(collector.inputPort.id).size());
  }

  public static class ActiveMQMultiPortInputOperator extends AbstractActiveMQInputOperator
  {
    @OutputPortFieldAnnotation(name = "outputPort1")
    public final transient DefaultOutputPort<String> outputPort1 = new DefaultOutputPort<String>(this);
    @OutputPortFieldAnnotation(name = "outputPort2")
    public final transient DefaultOutputPort<Integer> outputPort2 = new DefaultOutputPort<Integer>(this);

    /**
     * Implement abstract method.
     */
    @Override
    public void emitTuple(Message message)
    {
      //logger.debug("getTuple got called from {}", this);
      try {
        if (message instanceof TextMessage) {
          String msg = (String)((TextMessage)message).getText();
          outputPort1.emit(msg);
          logger.debug("emitted tuple : {}", msg);
        }
        else if (message instanceof StreamMessage) {
          Integer msg = new Integer(((StreamMessage)message).readInt());
          outputPort2.emit(msg);
        }
        else {
          throw new IllegalArgumentException("Unhandled message type " + message.getClass().getName());
        }
      }
      catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }
    }
  }

  @Test
  public void testActiveMQMultiPortInputOperator() throws Exception
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
    LogicalPlan dag = new LogicalPlan();
    // Create ActiveMQStringSinglePortInputOperator
    ActiveMQMultiPortInputOperator node = dag.addOperator("AMQ message consumer", ActiveMQMultiPortInputOperator.class);
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
    node.setTransacted(false);
    node.setVerbose(true);

    // Create Test tuple collector
    final CollectorModule2<String, Integer> collector = dag.addOperator("TestMessageCollector", new CollectorModule2<String, Integer>());

    // Connect ports
    dag.addStream("AMQ message", node.outputPort1, collector.inputPort1).setInline(true);
    dag.addStream("AMQ message2", node.outputPort2, collector.inputPort2).setInline(true);

    // Create and run local cluster
    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();
    WaitCondition c = new WaitCondition() {
      @Override
      public boolean isComplete() {
        if ( collections.size() > 1) {
          return collections.get(collector.inputPort1.id).size() >= 20;
        }
        else {
          return false;
        }
      }
    };
    StramTestSupport.awaitCompletion(c, 10000);
    lc.shutdown();

    // Check results
    Assert.assertEquals("Collections size", 2, collections.size());
    Assert.assertEquals("Tuple count", 20, collections.get(collector.inputPort1.id).size());
    Assert.assertEquals("Tuple count", 0, collections.get(collector.inputPort2.id).size());
  }
}
