/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.*;
import com.malhartech.dag.TestSink;
import com.malhartech.lib.testbench.EventGeneratorTest.CollectorInputPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.JMSException;
import javax.jms.Message;
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
public class ActiveMQOutputOperatorTest
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ActiveMQOutputOperatorTest.class);
  private static OperatorConfiguration config;
  private static BrokerService broker;

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    // config parameters should be in some configuration xml file as opposed to in code TBD
    config = new OperatorConfiguration(); // This is the master copy for configuration
    config.set("user", "");  // test done
    config.set("password", ""); // done
    config.set("url", "tcp://localhost:61617"); // done
    config.set("ackMode", "AUTO_ACKNOWLEDGE"); // done
    config.set("clientId", "Producer"); // done
    config.set("consumerName", "Consumer1");
    config.set("durable", "false"); // done
    config.set("maximumMessages", "30");  // 0 means unlimitted
    config.set("maximumSendMessages", "20");  // 0 means unlimitted
    config.set("maximumReceiveMessages", "20");  // 0 means unlimitted
    config.set("pauseBeforeShutdown", "true");
    config.set("receiveTimeOut", "0");
    config.set("sleepTime", "1000");
    config.set("subject", "TEST.FOO"); // done
    config.set("parallelThreads", "1");
    config.set("topic", "false");  // done
    config.set("transacted", "false");  // done
    config.set("verbose", "true");   // done
    config.set("batch", "10");
    config.set("messageSize", "225");

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
  public static void teardownClass() throws Exception
  {
    broker.stop();
  }

  /**
   * Concrete class of ActiveMQOutputOperator for testing.
   */
  public static class ActiveMQOutputOperator extends AbstractActiveMQOutputOperator<String>
  {
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
        msg = activeMQHelper.getSession().createTextMessage(obj);
      }
      catch (JMSException ex) {
        Logger.getLogger(ActiveMQOutputOperatorTest.class.getName()).log(Level.SEVERE, null, ex);
      }

      return msg;
    }
  }

  @Test
  @SuppressWarnings({"SleepWhileInLoop", "empty-statement"})
  public void testProducer() throws Exception
  {
    // [Generate tuple] ==> [send tuple through ActiveMQ output adapter(i.e. producer) into ActiveMQ message bus]
    //       ==> [receive data in outside ActiveMQ listener]

    // Setup a message listener to receive the message
    ActiveMQMessageListener listener = new ActiveMQMessageListener(config);
    try {
      listener.setupConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());;
    }
    listener.run();

    // Malhar module to send message
    ActiveMQOutputOperator node = new ActiveMQOutputOperator();
    node.setup(config);

    int numTuple = 10;
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
