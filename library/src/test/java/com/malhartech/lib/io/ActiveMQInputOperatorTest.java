/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestSink;
import java.io.IOException;
import java.util.HashMap;
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
  private static ActiveMQBase amqConfig;
  static int debugMessageCount = 5;  // for debug, display first few messages
  public HashMap<Integer, String> receivedData = new HashMap<Integer, String>();
  private int receivedCount = 0;

  /**
   * Concrete class of ActiveMQInputOperator for testing.
   */
  private final class ActiveMQInputOperator extends AbstractActiveMQInputOperator<String>
  {
    public ActiveMQInputOperator(ActiveMQBase helper)
    {
      super(helper);
    }

    @Override
    protected String getTuple(Message message) throws JMSException
    {
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
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    amqConfig = new ActiveMQBase(false); // false means not producer
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
    //amqConfig.cleanup();
    //broker.deleteAllMessages();
    //broker.removeDestination(amqConfig.getDestination());
    broker.stop();

  }

      }
      catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }
      generator.closeConnection();

      // The output port of node should be connected to a Test sink.
      TestSink<String> outSink = new TestSink<String>();
      ActiveMQInputOperator operator  = new ActiveMQInputOperator();
      operator.outputPort.setSink(outSink);
      operator.setup(config);

      int N = 10;
      int timeoutMillis = 5000;
      while (outSink.collectedTuples.size() < 10 && timeoutMillis > 0) {
        operator.replayEmitTuples(0);
        timeoutMillis -= 20;
        Thread.sleep(20);
      }

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
  @SuppressWarnings("SleepWhileInLoop")
  public void testActiveMQInputOperator() throws Exception
  {
    // Set configuation for ActiveMQ
    amqConfig.setUser("");
    amqConfig.setPassword("");
    amqConfig.setUrl("tcp://localhost:61617");
    amqConfig.setAckMode("CLIENT_ACKNOWLEDGE");
    amqConfig.setClientId("Client1");
    amqConfig.setConsumerName("Consumer1");
    amqConfig.setSubject("TEST.FOO");
    amqConfig.setMaximumMessage(20);
    amqConfig.setMessageSize(255);
    amqConfig.setBatch(10);
    amqConfig.setTopic(false);
    amqConfig.setDurable(false);
    //amqConfig.setTransacted(false);
    amqConfig.setVerbose(true);

    // Setup a message generator to receive the message from.
    ActiveMQMessageGenerator generator = new ActiveMQMessageGenerator(amqConfig);
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
    ActiveMQInputOperator node = new ActiveMQInputOperator(amqConfig);
    node.outputPort.setSink(outSink);
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

  }

  @Test
  public void test1() throws Exception
  {
    amqConfig.setTransacted(false);
    testActiveMQInputOperator();
    Thread.sleep(3000);
  }

  //@Test
  public void test2() throws Exception
  {
    amqConfig.setTransacted(true);
    testActiveMQInputOperator();
  }
}
