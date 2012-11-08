/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.*;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.stram.StramLocalCluster;
import com.malhartech.util.CircularBuffer;
import java.io.File;
import javax.jms.JMSException;
import javax.jms.Message;
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
public class ActiveMQOutputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQOutputOperatorTest.class);
  private static BrokerService broker;
  public static int tupleCount = 0;
  public static final transient int maxTuple = 20;

  /**
   * Concrete class of ActiveMQStringSinglePortOutputOperator for testing.
   */
  public static class ActiveMQStringSinglePortOutputOperator extends AbstractActiveMQSinglePortOutputOperator<String>
  {
    /**
     * Implementation of Abstract Method.
     *
     * @param tuple
     * @return
     */
    @Override
    protected Message createMessage(String tuple)
    {
      Message msg = null;
      try {
        msg = getSession().createTextMessage(tuple.toString());
      }
      catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }

      return msg;
    }
  } // End of ActiveMQStringSinglePortOutputOperator

  /**
   * Start ActiveMQ broker from the Testcase.
   *
   * @throws Exception
   */
  private void startActiveMQService() throws Exception
  {
    broker = new BrokerService();
    String brokerName = "ActiveMQOutputOperator-broker";
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
  }

  /**
   * Tuple generator for testing.
   */
  public static class StringGeneratorInputOperator implements InputOperator, ActivationListener<OperatorContext>
  {
    public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>(this);
    private final transient CircularBuffer<String> stringBuffer = new CircularBuffer<String>(1024);
    private volatile Thread dataGeneratorThread;

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    @Override
    public void activate(OperatorContext ctx)
    {
      dataGeneratorThread = new Thread("String Generator")
      {
        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run()
        {
          try {
            int i = 0;
            while (dataGeneratorThread != null && i < maxTuple) {
              stringBuffer.put("testString " + (++i));
              tupleCount++;
              Thread.sleep(20);
            }
          }
          catch (InterruptedException ie) {
          }
        }
      };
      dataGeneratorThread.start();
    }

    @Override
    public void deactivate()
    {
      dataGeneratorThread = null;
    }

    @Override
    public void emitTuples()
    {
      for (int i = stringBuffer.size(); i-- > 0;) {
        outputPort.emit(stringBuffer.pollUnsafe());
      }
    }
  }

  /**
   * Test AbstractActiveMQOutputOperator (i.e. an output adapter for ActiveMQ, aka producer).
   * This module sends data into an ActiveMQ message bus.
   *
   * [Generate tuple] ==> [send tuple through ActiveMQ output adapter(i.e. producer) into ActiveMQ message bus]
   * ==> [receive data in outside ActiveMQ listener]
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings({"SleepWhileInLoop", "empty-statement"})
  public void testActiveMQOutputOperator() throws Exception
  {
    // Setup a message listener to receive the message
    ActiveMQMessageListener listener = new ActiveMQMessageListener();
    try {
      listener.setupConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
    listener.run();

    // Malhar module to send message
    // Create DAG for testing.
    DAG dag = new DAG();

    // Create ActiveMQStringSinglePortOutputOperator
    StringGeneratorInputOperator generator = dag.addOperator("NumberGenerator", StringGeneratorInputOperator.class);
    ActiveMQStringSinglePortOutputOperator node = dag.addOperator("AMQ message producer", ActiveMQStringSinglePortOutputOperator.class);
    // Set configuration parameters for ActiveMQ
    node.setUser("");
    node.setPassword("");
    node.setUrl("tcp://localhost:61617");
    node.setAckMode("CLIENT_ACKNOWLEDGE");
    node.setClientId("Client1");
    node.setSubject("TEST.FOO");
    node.setMaximumSendMessages(15);
    node.setMessageSize(255);
    node.setBatch(10);
    node.setTopic(false);
    node.setDurable(false);
    node.setTransacted(false);
    node.setVerbose(true);

    // Connect ports
    dag.addStream("AMQ message", generator.outputPort, node.inputPort).setInline(true);

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

    // Check values send vs received
    long emittedCount = tupleCount < node.getMaximumSendMessages() ? tupleCount : node.getMaximumSendMessages();
    Assert.assertEquals("Number of emitted tuples", emittedCount, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.receivedData.get(new Integer(1)));

    listener.closeConnection();
  }

  @Test
  @SuppressWarnings({"SleepWhileInLoop", "empty-statement"})
  public void testActiveMQOutputOperator2() throws Exception
  {
    // Setup a message listener to receive the message
    ActiveMQMessageListener listener = new ActiveMQMessageListener();
    try {
      listener.setTopic(true);
      listener.setupConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
    listener.run();

    // Malhar module to send message
    // Create DAG for testing.
    DAG dag = new DAG();

    // Create ActiveMQStringSinglePortOutputOperator
    StringGeneratorInputOperator generator = dag.addOperator("NumberGenerator", StringGeneratorInputOperator.class);
    ActiveMQStringSinglePortOutputOperator node = dag.addOperator("AMQ message producer", ActiveMQStringSinglePortOutputOperator.class);
    // Set configuration parameters for ActiveMQ
    node.setUser("");
    node.setPassword("");
    node.setUrl("tcp://localhost:61617");
    node.setAckMode("CLIENT_ACKNOWLEDGE");
    node.setClientId("Client1");
    node.setSubject("TEST.FOO");
    node.setMaximumSendMessages(10);
    node.setMessageSize(255);
    node.setBatch(10);
    node.setTopic(true);
    node.setDurable(false);
    node.setTransacted(false);
    node.setVerbose(true);

    // Connect ports
    dag.addStream("AMQ message", generator.outputPort, node.inputPort).setInline(true);

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

    // Check values send vs received
    long emittedCount = tupleCount < node.getMaximumSendMessages() ? tupleCount : node.getMaximumSendMessages();
    Assert.assertEquals("Number of emitted tuples", emittedCount, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.receivedData.get(new Integer(1)));

    listener.closeConnection();
  }

  /**
   * Tuple generator for testing.
   */
  public static class StringGeneratorInputOperator2 implements InputOperator, ActivationListener<OperatorContext>
  {
    public final transient DefaultOutputPort<String> outputPort1 = new DefaultOutputPort<String>(this);
    public final transient DefaultOutputPort<Integer> outputPort2 = new DefaultOutputPort<Integer>(this);
    private final transient CircularBuffer<String> stringBuffer = new CircularBuffer<String>(1024);
    private final transient CircularBuffer<Integer> integerBuffer = new CircularBuffer<Integer>(1024);
    private volatile Thread dataGeneratorThread;

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    @Override
    public void activate(OperatorContext ctx)
    {
      dataGeneratorThread = new Thread("String Generator")
      {
        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run()
        {
          try {
            int i = 1;
            while (dataGeneratorThread != null && i <= maxTuple) {
              stringBuffer.put("testString " + i);
              integerBuffer.put(new Integer(i));
              tupleCount++;
              i++;
              Thread.sleep(20);
            }
          }
          catch (InterruptedException ie) {
          }
        }
      };
      dataGeneratorThread.start();
    }

    @Override
    public void deactivate()
    {
      dataGeneratorThread = null;
    }

    @Override
    public void emitTuples()
    {
      for (int i = stringBuffer.size(); i-- > 0;) {
        outputPort1.emit(stringBuffer.pollUnsafe());
      }
      for (int i = integerBuffer.size(); i-- > 0;) {
        outputPort2.emit(integerBuffer.pollUnsafe());
      }
    }
  }

  /**
   * Concrete class of ActiveMQStringSinglePortOutputOperator2 for testing.
   */
  public static class ActiveMQMultiPortOutputOperator extends AbstractActiveMQOutputOperator
  {
    /**
     * Two input ports.
     */
    @InputPortFieldAnnotation(name = "ActiveMQInputPort1")
    public final transient DefaultInputPort<String> inputPort1 = new DefaultInputPort<String>(this)
    {
      @Override
      public void process(String tuple)
      {
        try {
          Message msg = getSession().createTextMessage(tuple.toString());
          getProducer().send(msg);
          //logger.debug("process message {}", tuple.toString());
        }
        catch (JMSException ex) {
          logger.debug(ex.getLocalizedMessage());
        }
      }
    };
    @InputPortFieldAnnotation(name = "ActiveMQInputPort2")
    public final transient DefaultInputPort<Integer> inputPort2 = new DefaultInputPort<Integer>(this)
    {
      @Override
      public void process(Integer tuple)
      {
        try {
          Message msg = getSession().createTextMessage(tuple.toString());
          getProducer().send(msg);
          //logger.debug("process message {}", tuple.toString());
        }
        catch (JMSException ex) {
          logger.debug(ex.getLocalizedMessage());
        }
      }
    };
  } // End of ActiveMQMultiPortOutputOperator

  @Test
  @SuppressWarnings({"SleepWhileInLoop", "empty-statement"})
  public void testActiveMQMultiPortOutputOperator() throws Exception
  {
    // Setup a message listener to receive the message
    ActiveMQMessageListener listener = new ActiveMQMessageListener();
    try {
      listener.setMaximumReceiveMessages(0);
      listener.setupConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
    listener.run();

    // Malhar module to send message
    // Create DAG for testing.
    DAG dag = new DAG();

    // Create ActiveMQStringSinglePortOutputOperator
    StringGeneratorInputOperator2 generator = dag.addOperator("NumberGenerator", StringGeneratorInputOperator2.class);
    ActiveMQMultiPortOutputOperator node = dag.addOperator("AMQ message producer", ActiveMQMultiPortOutputOperator.class);
    // Set configuration parameters for ActiveMQ
    node.setUser("");
    node.setPassword("");
    node.setUrl("tcp://localhost:61617");
    node.setAckMode("CLIENT_ACKNOWLEDGE");
    node.setClientId("Client1");
    node.setSubject("TEST.FOO");
    node.setMaximumSendMessages(0);
    node.setMessageSize(255);
    node.setBatch(10);
    node.setTopic(false);
    node.setDurable(false);
    node.setTransacted(false);
    node.setVerbose(true);

    // Connect ports
    dag.addStream("AMQ message", generator.outputPort1, node.inputPort1).setInline(true);
    dag.addStream("AMQ message2", generator.outputPort2, node.inputPort2).setInline(true);

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

    // Check values send vs received
    long emittedCount = 40;
    Assert.assertEquals("Number of emitted tuples", emittedCount, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.receivedData.get(new Integer(1)));

    listener.closeConnection();
  }
}
