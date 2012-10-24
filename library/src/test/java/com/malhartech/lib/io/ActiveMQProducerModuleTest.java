/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.*;
import com.malhartech.lib.testbench.EventGeneratorTest.CollectorInputPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.JMSException;
import javax.jms.Message;
import org.apache.activemq.broker.BrokerService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class ActiveMQProducerModuleTest
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ActiveMQProducerModuleTest.class);
  //static AbstractActiveMQProducerModule node;
  private static OperatorConfiguration config;
  private static BrokerService broker;

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    //node = new ActiveMQProducerModule();

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
   * Concrete class of ActiveMQProducerModule for testing.
   */
  //@ModuleAnnotation(ports = {
  //  @PortAnnotation(name = ActiveMQProducerModule.INPUT, type = PortAnnotation.PortType.INPUT)
  //})

    public static class ProducerInputPort<T> extends DefaultInputPort<T>
  {
    ArrayList<T> list;
    final String id;

    public ProducerInputPort(String id, Operator module)
    {
      super(module);
      this.id = id;
    }

    @Override
    public void process(T tuple)
    {
      list.add(tuple);
    }
  }

  public static class ActiveMQProducerModule extends AbstractActiveMQProducerModule
  {
    /**
     * Abstract Method, needs to implement by every concrete ActiveMQProducerModule.
     *
     * @param obj
     * @return
     */
    @Override
    protected Message createMessage(Object obj)
    {
      //System.out.println("we are in createMessage");
      try {
        Message msg =  activeMQHelper.getSession().createTextMessage(obj.toString());
        return msg;
      }
      catch (JMSException ex) {
        Logger.getLogger(ActiveMQProducerModuleTest.class.getName()).log(Level.SEVERE, null, ex);
      }

      return null; // we can't return null TBD
    }
  }

    public static class ProducerOperator extends BaseOperator
  {
    public final transient ProducerInputPort<String> sdata = new ProducerInputPort<String>("sdata", this);

  }

  @Test
  @SuppressWarnings( {"SleepWhileInLoop", "empty-statement"})
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

        DAG dag = new DAG();
    ActiveMQProducerModule node = dag.addOperator("eventgen", ActiveMQProducerModule.class);
    ProducerOperator collector = dag.addOperator("data collector", new ProducerOperator());

    node.setKeys("a,b,c,d");
    node.setValues("");

    dag.addStream("stest", node.string_data, collector.sdata).setInline(true);


    node.setWeights("10,40,20,30");
    node.setTuplesBlast(10000000);
    node.setRollingWindowCount(5);
    node.setup(new OperatorConfiguration());
    Sink sink = node.connect(ActiveMQProducerModule.INPUT, node); // sink is writing to input port
    node.setup(config);

    final AtomicBoolean inactive = new AtomicBoolean(true);
    new Thread()
    {
      @Override
      public void run()
      {
        inactive.set(false);
        node.activate(new ModuleContext("ActiveMQProducerModuleTestNode", this));
      }
    }.start();


    // spin while the node gets activated.


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

    Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
    sink.process(bt);

    int numTuple = 10;
    for (int i = 0; i < numTuple; i++) {
      HashMap<String, Integer> sendData = new HashMap<String, Integer>();
      sendData.put("a", 2);
      sendData.put("b", 20);
      sendData.put("c", 1000);
      sink.process(sendData);
    }

    Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
    sink.process(et);

    // wait so that other side can receive data
    Thread.sleep(numTuple * 100 + 1);

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", numTuple, listener.receivedData.size());
    logger.debug(String.format("Processed %d tuples", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "{b=20, c=1000, a=2}", listener.receivedData.get(new Integer(1)));

    listener.closeConnection();
  }
}
