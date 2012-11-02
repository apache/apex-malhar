/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.*;
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
 *    @author Locknath Shil <locknath@malhar-inc.com>
 */
public class ActiveMQOutputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQOutputOperatorTest.class);
  private static BrokerService broker;
  public static int tupleCount = 0;
  public static final transient int maxTuple = 20;

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
    try {
      Thread.sleep(1000);
    }
    catch (InterruptedException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  /**
   *    Concrete class of ActiveMQStringOutputOperator for testing.
   */
  public static class ActiveMQStringOutputOperator extends AbstractActiveMQOutputOperator<String>
  {
    /**
     *    Abstract Method, needs to implement by every concrete ActiveMQStringOutputOperator.
     *
     *    @param obj
     *    @return
     */
    @Override
    protected Message createMessage(String obj)
    {
      //logger.debug("createMessage got called in {}", this);
      Message msg = null;
      try {
        msg = getSession().createTextMessage(obj);
      }
      catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }

      return msg;
    }
    /**
     *    Define input ports as needed.
     */
    @InputPortFieldAnnotation(name = "ActiveMQInputPort")
    public final transient DefaultInputPort<String> inputPort = new DefaultInputPort<String>(this)
    {
      @Override
      public void process(String t)
      {
        logger.debug("process got called from {}", this);
        countMessages++;
        // Stop sending messages after max limit.
        if (countMessages > maxSendMessage && maxSendMessage != 0) {
          if (countMessages == maxSendMessage + 1) {
            logger.warn("Reached maximum send messages of {}", maxSendMessage);
          }
          return;
        }

        try {
          Message msg = createMessage(t);
          getProducer().send(msg);

          logger.debug("process got called from {} with message {}", this, t.toString());
        }
        catch (JMSException ex) {
          logger.debug(ex.getLocalizedMessage());
        }
      }
    };
  }

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
    public void postActivate(OperatorContext ctx)
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
    public void preDeactivate()
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
   *    Test AbstractActiveMQOutputOperator (i.e. an output adapter for ActiveMQ, aka producer).
   *    This module sends data into an ActiveMQ message bus.
   *
   *    [Generate tuple] ==> [send tuple through ActiveMQ output adapter(i.e. producer) into ActiveMQ message bus]
   *    ==> [receive data in outside ActiveMQ listener]
   *
   *    @throws Exception
   */
  //@Test
  @SuppressWarnings({"SleepWhileInLoop", "empty-statement"})
  public void testActiveMQOutputOperatorWithoutUsingDAG() throws Exception
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
    ActiveMQStringOutputOperator node = new ActiveMQStringOutputOperator();
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

    node.setup(new com.malhartech.dag.OperatorContext("irrelevant", null));

    long numTuple = node.getMaximumSendMessages();
    for (int i = 0; i < numTuple; i++) {
      String str = "teststring " + (i + 1);
      node.inputPort.process(str);
    }

    // wait so that other side can receive data
    Thread.sleep(numTuple * 100 + 1);

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", numTuple, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "teststring 1", listener.receivedData.get(new Integer(1)));

    listener.closeConnection();
  }

  @Test
  @SuppressWarnings({"SleepWhileInLoop", "empty-statement"})
  public void testActiveMQOutputOperator2() throws Exception
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

    // Create ActiveMQStringOutputOperator
    StringGeneratorInputOperator generator = dag.addOperator("NumberGenerator", StringGeneratorInputOperator.class);
    ActiveMQStringOutputOperator node = dag.addOperator("AMQ message producer", ActiveMQStringOutputOperator.class);
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
}
