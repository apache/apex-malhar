/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestSink;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import junit.framework.Assert;
import org.apache.activemq.broker.BrokerService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class ActiveMQInputOperatorTest
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ActiveMQInputOperatorTest.class);
  static OperatorConfiguration config;
 // static AbstractActiveMQInputOperator node;
  private static BrokerService broker;
  static int debugMessageCount = 5;  // for debug, display first few messages

  /**
   * Concrete class of ActiveMQInputOperator for testing.
   */
  private static final class ActiveMQInputOperator extends AbstractActiveMQInputOperator<String>
  {
    @Override
    protected String getTuple(Message message) throws JMSException
    {
      //logger.info("emitMessage got called from {}", this);
      if (message instanceof TextMessage) {
        logger.info("Received Message: {}", ((TextMessage)message).getText());
        return (((TextMessage)message).getText());
      }
      else {
        throw new JMSException("Unexpected message type of " + message.getClass());
      }
    }
  }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
      config = new OperatorConfiguration();
      config.set("user", "");
      config.set("password", "");
      config.set("url", "tcp://localhost:61617");
      config.set("ackMode", "AUTO_ACKNOWLEDGE");
      config.set("clientId", "consumer1");
      config.set("consumerName", "MyConsumer");
      config.set("durable", "false");
      config.set("maximumMessages", "10");  // 0 means unlimitted
      config.set("pauseBeforeShutdown", "true");
      config.set("receiveTimeOut", "0");
      config.set("sleepTime", "1000");
      config.set("subject", "TEST.FOO");
      config.set("parallelThreads", "1");
      config.set("topic", "false");
      config.set("transacted", "false");
      config.set("verbose", "true");
      config.set("batch", "10");
      config.set("messageSize", "225");

      //node = new ActiveMQInputOperator();

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
      broker.addConnector(config.get("url") + "?broker.persistent=false");
      broker.start();
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
      //node = null;
      config = null;
    }

    /**
     * Test AbstractActiveMQInputOperator (i.e. an input adapter for ActiveMQ, aka consumer).
     * This module receives data from an outside test generator through ActiveMQ message bus and
     * feed that data into Malhar streaming platform.
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("SleepWhileInLoop")
    public void testConsumer() throws Exception
    {
      // Setup a message generator to receive the message from.
      ActiveMQMessageGenerator generator = new ActiveMQMessageGenerator(config);
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
      ActiveMQInputOperator node  = new ActiveMQInputOperator();
      node.outputPort.setSink(outSink);
      node.setup(config);

      int N = 10;
      int timeoutMillis = 5000;
      while (outSink.collectedTuples.size() < 10 && timeoutMillis > 0) {
        node.injectTuples(0);
        timeoutMillis -= 20;
        Thread.sleep(20);
      }

      // Check values send vs received
      int totalCount = outSink.collectedTuples.size();
      Assert.assertEquals("Number of emitted tuples", generator.sendCount, totalCount);
      logger.debug(String.format("Number of emitted tuples: %d", totalCount));

      // Check contents only for first few.
      for (int i = 0; i < debugMessageCount & i < totalCount; ++i) {
        Assert.assertEquals("Message content", generator.sendData.get(i), (String)(outSink.collectedTuples.get(i)));
        logger.debug(String.format("Received: %s", outSink.collectedTuples.get(i)));
      }

    }
  }
