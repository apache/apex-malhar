/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.zmq;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.contrib.zmq.AbstractZeroMQOutputOperator;
import com.malhartech.dag.*;
import com.malhartech.stream.StramTestSupport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.*;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class OutputZeroMQModuleTest
{
  static OperatorConfiguration config;
  static AbstractZeroMQOutputOperator node;
  static com.malhartech.contrib.zmq.OutputZeroMQModuleTest.MyStreamContext context;
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(OutputZeroMQModuleTest.class);
  int testNum = 10;
  private static final class MyStreamContext extends ModuleContext implements Sink
  {
    public MyStreamContext()
    {
      super("irrelevant_id", Thread.currentThread());  // the 2nd argument could be wrong. Please check.
    }

    @Override
    public void process(Object payload)
    {
      System.out.print("processing ".concat(payload.toString()));
    }
  }

  private static final class OutputZeroMQStream extends AbstractZeroMQOutputOperator
  {
/*
    @Override
    public void beginWindow()
    {
    }

    @Override
    public void endWindow()
    {
    }
    @Override
    public void process(Object payload)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    */
  }

  private static final class InputZeroMQStream
  {
    protected ZMQ.Context context;
    protected ZMQ.Socket subscriber;
    public void setup(OperatorConfiguration config) throws FailedOperationException
    {
      context = ZMQ.context(1);
      logger.debug("Subsribing on ZeroMQ");
      subscriber = context.socket(ZMQ.SUB);
      subscriber.connect("tcp://localhost:5556");

      String filter = "10001";
      filter = "";
      subscriber.subscribe(filter.getBytes());
    }

    public void process(Object payload)
    {
        byte[] message = subscriber.recv(0);
        String string = new String(subscriber.recv(0)).trim();
    }

    public void teardown()
    {
        subscriber.close();
        context.term();
    }
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    config = new OperatorConfiguration();
    config.set("user", "");
    config.set("password", "");
    config.set("url", "tcp://*:5556");
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

    node = new OutputZeroMQStream();

    context = new MyStreamContext();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    node = null;
    config = null;
    context = null;
  }
/*
  @Before
  public void setUp() throws Exception
  {
    //System.out.println("setup");
    instance.setup(config);
  }

  @After
  public void tearDown()
  {
    //System.out.println("teardown");
    instance.teardown();
  }
  */

  public class SubscriberThread extends Thread {
      ArrayList<String> recvList = null;
      InputZeroMQStream subscriber = null;
      public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
      public int count =0;
      public void setRecvList(ArrayList<String> recvList) {
        this.recvList = recvList;
      }
      public void setSubscriber( InputZeroMQStream subscriber) {
        this.subscriber = subscriber;
      }
      @Override
      public void run() {
        while(true){
          byte[] msg = subscriber.subscriber.recv(0);
          // convert to HashMap and save the values for each key
          // then expect c to be 1000, b=20, a=2
          // and do count++ (where count now would be 30)
          String str = new String(msg);
          int eq = str.indexOf('=');
          String key = str.substring(1, eq);
          int value = Integer.parseInt(str.substring(eq+1, str.length()-1));
//          logger.debug("\nsubscriber recv:"+str);
          dataMap.put(key, value);
          count++;
        }
      }
  }

  @Test
  public void testProcess() throws InterruptedException
  {
    InputZeroMQStream subscriber = new InputZeroMQStream();
    OperatorConfiguration conf1 = new OperatorConfiguration();
    subscriber.setup(conf1);
//    ArrayList<String> recvList = new ArrayList<String>();
    SubscriberThread subThr = new SubscriberThread();
//    subThr.setRecvList(recvList);
    subThr.setSubscriber(subscriber);
    subThr.start();

    //OutputZeroMQStream publisher = new OutputZeroMQStream();
    //Sink testSink = node.connect(OutputZeroMQStream.INPUT, node); // sink is before the node
    com.malhartech.api.Sink testSink = node.input.getSink();
    node.setup(config);

    long begin = System.currentTimeMillis();
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

    while( subThr.count < testNum*3 )
      Thread.sleep(1);
    junit.framework.Assert.assertEquals("emitted value for testNum was ", testNum*3, subThr.count);
    for (Map.Entry<String, Integer> e: subThr.dataMap.entrySet()) {
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
    long end = System.currentTimeMillis();
//    logger.debug("execution time is "+(end-begin)+" ms");
    logger.debug("end of test");
  }
}
