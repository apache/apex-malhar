/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.zmq;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.*;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class ZeroMQOutputOperatorTest
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(ZeroMQOutputOperatorTest.class);

  private static final class TestStringZeroMQOutputOperator extends AbstractZeroMQOutputOperator<String>
  {
    private int testNum;

    public void processTuple(String tuple)
    {
      publisher.send(tuple.getBytes(), 0);
    }

    public void setTestNum(int testNum)
    {
      this.testNum = testNum;
    }

    public void generateMessages()
    {
      Sink testSink = input.getSink();
      beginWindow();
      for (int i = 0; i < testNum; i++) {
        HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
        dataMapa.put("a", 2);
        testSink.process(dataMapa.toString());

        HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
        dataMapb.put("b", 20);
        testSink.process(dataMapb.toString());

        HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
        dataMapc.put("c", 1000);
        testSink.process(dataMapc.toString());
      }
      endWindow();
      teardown();
    }
  }

  private static final class ZeroMQMessageReceiver implements Runnable
  {
    public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    public int count = 0;
    protected ZMQ.Context context;
    protected ZMQ.Socket subscriber;
    protected ZMQ.Socket syncclient;

    public void setup()
    {
      context = ZMQ.context(1);
      logger.debug("Subsribing on ZeroMQ");
      subscriber = context.socket(ZMQ.SUB);
      subscriber.connect("tcp://localhost:5556");
      subscriber.subscribe("".getBytes());
      syncclient = context.socket(ZMQ.REQ);
      syncclient.connect("tcp://localhost:5557");
      syncclient.send("".getBytes(), 0);
    }

    @Override
    public void run()
    {
      while (true) {
        byte[] msg = subscriber.recv(0);
        // convert to HashMap and save the values for each key
        // then expect c to be 1000, b=20, a=2
        // and do count++ (where count now would be 30)
        String str = new String(msg);
        if (str.indexOf("{") == -1) {
          continue;
        }
        int eq = str.indexOf('=');
        String key = str.substring(1, eq);
        int value = Integer.parseInt(str.substring(eq + 1, str.length() - 1));
//          logger.debug("\nsubscriber recv:"+str);
        dataMap.put(key, value);
        count++;
      }
    }

    public void teardown()
    {
      subscriber.close();
      context.term();
    }
  }

  @Test
  public void testProcess() throws InterruptedException, MalformedURLException
  {
    final int testNum = 10;
    new Thread()
    {
      public void run()
      {
        TestStringZeroMQOutputOperator node = new TestStringZeroMQOutputOperator();
        node.setUrl("tcp://*:5556");
        node.setSyncUrl("tcp://*:5557");
        node.setup(new OperatorConfiguration());
        node.setSUBSCRIBERS_EXPECTED(1);
        node.setTestNum(testNum);
        node.startSyncJob();
        node.generateMessages();
      }
    }.start();

    ZeroMQMessageReceiver receiver = new ZeroMQMessageReceiver();
    receiver.setup();
    Thread subThr = new Thread(receiver);
    subThr.start();

    while (receiver.count < testNum * 3) {
      Thread.sleep(1);
    }
    junit.framework.Assert.assertEquals("emitted value for testNum was ", testNum * 3, receiver.count);
    for (Map.Entry<String, Integer> e: receiver.dataMap.entrySet()) {
      if (e.getKey().equals("a")) {
        junit.framework.Assert.assertEquals("emitted value for 'a' was ", new Integer(2), e.getValue());
      }
      else if (e.getKey().equals("b")) {
        junit.framework.Assert.assertEquals("emitted value for 'b' was ", new Integer(20), e.getValue());
      }
      else if (e.getKey().equals("c")) {
        junit.framework.Assert.assertEquals("emitted value for 'c' was ", new Integer(1000), e.getValue());
      }
    }
    subThr.interrupt();
    logger.debug("end of test emitted "+testNum * 3+" msgs");
  }
}
