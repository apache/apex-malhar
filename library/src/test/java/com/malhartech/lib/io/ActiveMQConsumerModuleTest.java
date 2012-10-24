/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;


import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.WindowGenerator;
import com.malhartech.stram.ManualScheduledExecutorService;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import org.apache.activemq.broker.BrokerService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class ActiveMQConsumerModuleTest
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ActiveMQConsumerModuleTest.class);
  static OperatorConfiguration config;
  static AbstractActiveMQConsumerModule node;
  private static BrokerService broker;
  int debugMessageCount=5;  // for debug, display first few messages

  /**
   * Concrete class of ActiveMQConsumerModule for testing.
   */
  //@ModuleAnnotation(ports = {
  //  @PortAnnotation(name = ActiveMQConsumerModule.OUTPUT, type = PortAnnotation.PortType.OUTPUT)
  //})
  private static final class ActiveMQConsumerModule extends AbstractActiveMQConsumerModule
  {
   // public int tupleCount = this.generatedTupleCount;

    @Override
    protected void emitMessage(Message message)
    {
      //logger.info("emitMessage got called from {}", this);
      if (message instanceof TextMessage) {
        try {
          emit(Component.OUTPUT, ((TextMessage)message).getText());
          logger.info("Received Message: {}", ((TextMessage)message).getText());
        }
        catch (JMSException ex) {
          Logger.getLogger(ActiveMQConsumerModuleTest.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    }

    @Override
    public Runnable getDataPoller()
    {
      throw new UnsupportedOperationException("Not supported yet.");
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

    node = new ActiveMQConsumerModule();

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
    node = null;
    config = null;
  }

  /**
   * Test AbstractActiveMQConsumerModule (i.e. an input adapter for ActiveMQ, aka consumer).
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

    // Setup winddow generator. This is needed to supply data into ActiveMQConsumerModule(i.e. node in here).
    final ManualScheduledExecutorService mses = new ManualScheduledExecutorService(1);
    final WindowGenerator wingen = new WindowGenerator(mses);
    config.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, 0);
    config.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 1);
//    wingen.setup(config);

    // The input port of node should be connected to window generator and
    // the output port of node should be connected to a Test sink.
  /*  Sink inSink = node.connect(Component.INPUT, wingen);
    wingen.connect("mytestnode", inSink);
    TestSink outSink = new TestSink();
    node.connect(ActiveMQConsumerModule.OUTPUT, outSink);
    node.setup(config);

    // Activate the node
    final AtomicBoolean inactive = new AtomicBoolean(true);
    new Thread()
    {
      @Override
      public void run()
      {
        node.activate(new ModuleContext("ActiveMQConsumerModuleTestNode", this));
        inactive.set(false);
      }
    }.start();

    // Spin to allow the node gets activated. Do we always need this?? TBD
    int sleeptimes = 0;
    try {
      do {
        Thread.sleep(20);
        sleeptimes++;
        if (sleeptimes > 5) {
          break;
        }
      }
      while (inactive.get());
    }
    catch (InterruptedException ex) {
      logger.debug(ex.getLocalizedMessage());
    }

    // Activate wingen
    wingen.activate(null);

    for (int i = 0; i < 500; i++) {
      mses.tick(1);
      try {
        Thread.sleep(1);
      }
      catch (InterruptedException e) {
        logger.error("Unexpected error while sleeping for 1 s", e);
      }
    }

    node.deactivate(); // is this should be here or after generator? TBD

    // Delay a bit to make sure we can receive data.
    try {
      for (int i = 0; i < 50; i++) {
        Thread.sleep(20);
        if (outSink.collectedTuples.size() >= 1) {
          break;
        }
      }
    }
    catch (InterruptedException ex) {
      logger.debug(ex.getLocalizedMessage());
    }

    // Check values send vs received
    int totalCount = outSink.collectedTuples.size();
    Assert.assertEquals("Number of emitted tuples", generator.sendCount, totalCount);
    logger.debug(String.format("Number of emitted tuples: %d", totalCount));

    // Check contents only for first few.
    for (int i=0; i<debugMessageCount & i<totalCount; ++i) {
      Assert.assertEquals("Message content", generator.sendData.get(i), (String)(outSink.collectedTuples.get(i)));
      logger.debug(String.format("Received: %s", outSink.collectedTuples.get(i)));
    }
    */
  }
}
