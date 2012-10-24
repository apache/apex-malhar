/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.zmq;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestSink;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class ZeroMQInputOperatorTest
{
  static ArrayList<String> recvList = new ArrayList<String>();
  private static Logger logger = LoggerFactory.getLogger(ZeroMQInputOperatorTest.class);

  private static final class TestZeroMQInputOperator extends AbstractZeroMQInputOperator<String>
  {
    @Override
    public void emitMessage(byte[] message) {
//    logger.debug(new String(payload));
      outputPort.emit(new String(message));
   }
  }

  private static final class ZeroMQMessageGenerator
  {
    private ZMQ.Context context;
    private ZMQ.Socket publisher;
    private String addr;

    public void setup()
    {
      context = ZMQ.context(1);
      logger.debug("Publishing on ZeroMQ");
      publisher = context.socket(ZMQ.PUB);
      addr = "tcp://*:5556";
      publisher.bind(addr);
    }

    public void process(Object payload)
    {
      String msg = payload.toString();
      // logger.debug("publish:"+msg);
      publisher.send(msg.getBytes(), 0);
    }

    public void teardown()
    {
      publisher.close();
      context.term();
    }

    public void generateMessages(int msgCount) throws InterruptedException {
      Thread.sleep(100); // TODO: should not be here
      for( int i=0; i<msgCount; i++ ) {
        HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
        dataMapa.put("a", 2);
        process(dataMapa);

        HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
        dataMapb.put("b", 20);
        process(dataMapb);

        HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
        dataMapc.put("c", 1000);
        process(dataMapc);
      }
    }

  }

  @Test
  public void testProcess() throws Exception {

    final TestZeroMQInputOperator node = new TestZeroMQInputOperator();
    final int testNum = 3;

    TestSink<String> testSink = new TestSink<String>();
    node.outputPort.setSink(testSink);

/*
    config = new OperatorConfiguration();
    config.set("user", "");
    config.set("password", "");
    config.set("url", "tcp://localhost:5556");
    config.set("filter", "");
    config.set("ackMode", "AUTO_ACKNOWLEDGE");
    config.set("clientId", "consumer1");
    config.set("consumerName", "ChetanConsumer");
    config.set("durable", "false");
    config.set("maximumMessages", "10");
    config.set("pauseBeforeShutdown", "true");
    config.set("receiveTimeOut", "0");
    config.set("sleepTime", "1000");
    config.set("subject", "TEST.FOO");
    config.set("parallelThreads", "1");
    config.set("topic", "false");
    config.set("transacted", "false");
    config.set("verbose", "true");
    config.set("batch", "10");
*/
    node.setFilter("");
    node.setUrl(new URL("tcp://localhost:5556"));

    node.setup(new OperatorConfiguration());

    Thread nodeThread = new Thread()
    {
      @Override
      public void run()
      {
        node.getDataPoller().run();
      }
    };
    nodeThread.start();

    Thread generatorThread = new Thread()
    {
      @Override
      public void run()
      {
        // data generator with separate thread to external zmq server
        ZeroMQMessageGenerator publisher = new ZeroMQMessageGenerator();
        publisher.setup();
        try {
          publisher.generateMessages(testNum);
        } catch (InterruptedException ex) {
          logger.debug("generator exiting", ex);
        } finally {
          publisher.teardown();
        }
      }
    };
    generatorThread.start();

    testSink.waitForResultCount(testNum*3, 3000);
    Assert.assertTrue("tuple emmitted", testSink.collectedTuples.size() > 0);

    Assert.assertEquals("emitted value for testNum was ", testNum * 3, testSink.collectedTuples.size());
    for (int i = 0; i < testSink.collectedTuples.size(); i++) {
      String str = testSink.collectedTuples.get(i);
      int eq = str.indexOf('=');
      String key = str.substring(1, eq);
      Integer value = Integer.parseInt(str.substring(eq + 1, str.length() - 1));
      if (key.equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Integer(2), value);
      }
      else if (key.equals("b")) {
        Assert.assertEquals("emitted value for 'b' was ", new Integer(20), value);
      }
      if (key.equals("c")) {
        Assert.assertEquals("emitted value for 'c' was ", new Integer(1000), value);
      }
    }

    //long end = System.currentTimeMillis();
    // logger.debug("execution time:"+(end-begin)+" ms");

    generatorThread.interrupt();
    node.teardown();
    logger.debug("end of test sent " + testSink.collectedTuples.size() + " messages");

  }
}
