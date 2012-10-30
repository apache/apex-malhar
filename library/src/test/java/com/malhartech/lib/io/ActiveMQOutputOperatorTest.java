/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.OperatorConfiguration;
import javax.jms.JMSException;
import javax.jms.Message;
import junit.framework.Assert;
import org.apache.activemq.broker.BrokerService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
  //private static ActiveMQProducerBase amqConfig;

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    //amqConfig = new ActiveMQProducerBase();  // this is the producer

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
    broker.getSystemUsage().getStoreUsage().setLimit(1024 * 1024 * 1024);
    broker.getSystemUsage().getTempUsage().setLimit(100 * 1024 * 1024);
    broker.setDeleteAllMessagesOnStartup(true);
    broker.start();
  }

  @AfterClass
  public static void teardownClass() throws Exception
  {
    broker.stop();
    //amqConfig.cleanup();
  }

  /**
   * Concrete class of ActiveMQOutputOperator for testing.
   */
  public static class ActiveMQOutputOperator extends AbstractActiveMQOutputOperator<String>
  {
    public ActiveMQOutputOperator()
    {
    }

    /**
     * Abstract Method, needs to implement by every concrete ActiveMQOutputOperator.
     *
     * @param obj
     * @return
     */
    @Override
    protected Message createMessage(String obj)
    {
      //System.out.println("we are in createMessage");
      Message msg = null;
      try {
        msg = amqProducer.getSession().createTextMessage(obj);
      }
      catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }

      return msg;
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
    ActiveMQOutputOperator node = new ActiveMQOutputOperator();
        // Set configuation for ActiveMQ
    ActiveMQProducerBase amqConfig = node.getAmqProducer();
    amqConfig.setUser("");
    amqConfig.setPassword("");
    amqConfig.setUrl("tcp://localhost:61617");
    amqConfig.setAckMode("CLIENT_ACKNOWLEDGE");
    amqConfig.setClientId("Client1");
    amqConfig.setSubject("TEST.FOO");
    //amqConfig.setMaximumMessage(100);
    amqConfig.setMaximumSendMessages(15);
    //amqConfig.setMaximumReceiveMessages(100);
    amqConfig.setMessageSize(255);
    amqConfig.setBatch(10);
    amqConfig.setTopic(false);
    amqConfig.setDurable(false);
    amqConfig.setTransacted(false);
    amqConfig.setVerbose(true);

    node.setup(new OperatorConfiguration());

    long numTuple = amqConfig.getMaximumSendMessages();
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
}
