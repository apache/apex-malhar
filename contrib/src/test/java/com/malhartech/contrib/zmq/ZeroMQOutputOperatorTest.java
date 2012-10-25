/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.zmq;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.*;
import com.malhartech.stream.StramTestSupport;
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

  private static final class TestZeroMQInputOperator extends AbstractZeroMQOutputOperator<String>
  {

  }

  private static final class ZeroMQMessageReceiver implements Runnable
  {
    public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    public int count = 0;
    protected ZMQ.Context context;
    protected ZMQ.Socket subscriber;
    public void setup()
    {
      context = ZMQ.context(1);
      logger.debug("Subsribing on ZeroMQ");
      subscriber = context.socket(ZMQ.SUB);
      subscriber.connect("tcp://localhost:5556");
      String filter = "";
      subscriber.subscribe(filter.getBytes());
    }

    @Override
    public void run() {
      while(true){
        byte[] msg = subscriber.recv(0);
        // convert to HashMap and save the values for each key
        // then expect c to be 1000, b=20, a=2
        // and do count++ (where count now would be 30)
        String str = new String(msg);
        if( str.indexOf("{") == -1 ) {
          continue;
        }
        int eq = str.indexOf('=');
        String key = str.substring(1, eq);
        int value = Integer.parseInt(str.substring(eq+1, str.length()-1));
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
    int testNum = 10;
    ZeroMQMessageReceiver receiver = new ZeroMQMessageReceiver();
    receiver.setup();
    Thread subThr = new Thread(receiver);
    subThr.start();

    TestZeroMQInputOperator node = new TestZeroMQInputOperator();
    node.setUrl(new String("tcp://*:5556"));
    node.setup(new OperatorConfiguration());
    Sink testSink = node.input.getSink();
    Thread.sleep(500);
//    long begin = System.currentTimeMillis();
    Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
    testSink.process(bt);
    for( int i=0; i<testNum; i++ ) {
      HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
      dataMapa.put("a", 2);
      testSink.process(dataMapa);

      HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
      dataMapb.put("b", 20);
      testSink.process(dataMapb);

      HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
      dataMapc.put("c", 1000);
      testSink.process(dataMapc);
    }
    Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
    testSink.process(et);

    while( receiver.count < testNum*3 ) {
      Thread.sleep(1);
    }
    junit.framework.Assert.assertEquals("emitted value for testNum was ", testNum*3, receiver.count);
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
    node.teardown();
//    long end = System.currentTimeMillis();
//    logger.debug("execution time is "+(end-begin)+" ms");
    logger.debug("end of test");
  }
}
