/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.zmq;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import com.malhartech.dag.OperatorContext;
import com.malhartech.dag.TestSink;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class ZeroMQInputOperatorTest
{
  String pubAddr = "tcp://*:5556";
  String syncAddr = "tcp://*:5557";
  private static Logger logger = LoggerFactory.getLogger(ZeroMQInputOperatorTest.class);

  private static final class TestStringZeroMQInputOperator extends AbstractZeroMQInputOperator<String>
  {
    @Override
    public void emitMessage(byte[] message)
    {
//    logger.debug(new String(payload));
      outputPort.emit(new String(message));
    }

    public void replayTuples(long windowId)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  private final class ZeroMQMessageGenerator
  {
    private ZMQ.Context context;
    private ZMQ.Socket publisher;
    private ZMQ.Socket syncservice;
    private final int SUBSCRIBERS_EXPECTED = 1;

    public void setup()
    {
      context = ZMQ.context(1);
      logger.debug("Publishing on ZeroMQ");
      publisher = context.socket(ZMQ.PUB);
      publisher.bind(pubAddr);
      syncservice = context.socket(ZMQ.REP);
      syncservice.bind(syncAddr);
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

    public void generateMessages(int msgCount) throws InterruptedException
    {
      for (int subscribers = 0; subscribers < SUBSCRIBERS_EXPECTED; subscribers++) {
        byte[] value = syncservice.recv(0);
        syncservice.send("".getBytes(), 0);
      }
      for (int i = 0; i < msgCount; i++) {
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
  public void testProcess() throws Exception
  {
    final int testNum = 3;
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
        }
        catch (InterruptedException ex) {
          logger.debug("generator exiting", ex);
        }
        finally {
          publisher.teardown();
        }
      }
    };
    generatorThread.start();

    final TestStringZeroMQInputOperator node = new TestStringZeroMQInputOperator();

    TestSink<String> testSink = new TestSink<String>();
    node.outputPort.setSink(testSink);

    node.setFilter("");
    node.setUrl("tcp://localhost:5556");
    node.setSyncUrl("tcp://localhost:5557");

    node.setup(new OperatorConfiguration());

    Thread nodeThread = new Thread()
    {
      @Override
      public void run()
      {
        node.setup(new OperatorContext("irrelevant", null));
        node.activate(null);
        try {
          while (true) {
            node.emitTuples();
            Thread.sleep(10);
          }
        }
        catch (InterruptedException ex) {
        }

        node.deactivate();
        node.teardown();
      }
    };
    t.start();

    testSink.waitForResultCount(testNum * 3, 3000);
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
