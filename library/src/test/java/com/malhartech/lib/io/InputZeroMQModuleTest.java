/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.*;
import com.malhartech.stram.ManualScheduledExecutorService;
import com.malhartech.stram.WindowGenerator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.junit.*;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class InputZeroMQModuleTest
{
  static OperatorConfiguration config;
  static InputZeroMQStream node;
  static com.malhartech.lib.io.InputZeroMQModuleTest.MyStreamContext context;
  static ArrayList<String> recvList = new ArrayList<String>();
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(InputZeroMQModuleTest.class);
  int testNum = 3;
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

  private static final class InputZeroMQStream extends AbstractZeroMQInputOperator
  {
    public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    public int count =0;

    @Override
    public void emitMessage(byte[] payload) {
//    logger.debug(new String(payload));
      outputPort.emit(new String(payload));
   }
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    node = new InputZeroMQStream();
    context = new MyStreamContext();

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
    node.setup(config);
  }

  @After
  public void tearDown()
  {
    //System.out.println("teardown");
    node.teardown();
  }
*/
   private static final class OutputZeroMQStream
  {
      private ZMQ.Context context;
      private ZMQ.Socket publisher;
      private String addr;
      public void setup(ModuleConfiguration config) throws FailedOperationException
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
//        logger.debug("publish:"+msg);
        publisher.send(msg.getBytes(), 0);

      }
      public void teardown()
      {
          publisher.close();
          context.term();
      }
   }

  public class PublisherThread extends Thread {
    OutputZeroMQStream publisher = null;
    public void setPublisher(OutputZeroMQStream publisher) {
      this.publisher = publisher;
    }
    @Override
    public void run() {
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException ex) {
        Logger.getLogger(InputZeroMQModuleTest.class.getName()).log(Level.SEVERE, null, ex);
      }

//      Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
//      publisher.process(bt);

      for( int i=0; i<testNum; i++ ) {
        HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
        dataMapa.put("a", 2);
        publisher.process(dataMapa);

        HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
        dataMapb.put("b", 20);
        publisher.process(dataMapb);


        HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
        dataMapc.put("c", 1000);
        publisher.process(dataMapc);
      }
//      Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
//      publisher.process(et);
    }
  }

  @Test
  public void testProcess() throws InterruptedException {

        final ManualScheduledExecutorService mses = new ManualScheduledExecutorService(1);
        final WindowGenerator wingen = new WindowGenerator(mses);

        Configuration winconf = new Configuration();
        winconf.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, 0);
        winconf.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 1);
        wingen.setup(winconf);

//        Sink input = node.connect(Component.INPUT, wingen);
//        wingen.connect("mytestnode", input);


        TestSink testSink = new TestSink();
        node.outputPort.setSink(testSink);
//        node.connect(InputZeroMQStream.OUTPUT, testSink);
        node.setup(config);
    Thread nodeThread = new Thread()
    {
      @Override
      public void run()
      {
        node.getDataPoller().run();
      }
    };
    nodeThread.start();
//        final AtomicBoolean inactive = new AtomicBoolean(true);
//        new Thread() {
//            @Override
//            public void run() {
//                node.activate(new ModuleContext("InputZeroMQTestNode", this));
//                inactive.set(false);
//            }
//        }.start();
//        /**
//         * spin while the node gets activated.
//         */
//        try {
//            int i=0;
//            do {
//                Thread.sleep(20);
//                if( i++ > 5)
//                  break;
//            } while (inactive.get());
//        } catch (InterruptedException ex) {
//            logger.debug(ex.getLocalizedMessage());
//        }

        OutputZeroMQStream publisher = new OutputZeroMQStream();
      ModuleConfiguration conf1 = new ModuleConfiguration("publisher", new HashMap<String, String>());
      publisher.setup(conf1);

      long begin = System.currentTimeMillis();
      PublisherThread pubThr = new PublisherThread();
      pubThr.setPublisher(publisher);
      pubThr.start();

        wingen.activate(null);

//        for (int i = 0; i < 500; i++) {
//            mses.tick(1);
//            try {
//                Thread.sleep(1);
//            } catch (InterruptedException e) {
//                logger.error("Unexpected error while sleeping for 1 s", e);
//            }
//        }
//        node.deactivate();

    Thread.yield();
    while (nodeThread.getState() != Thread.State.RUNNABLE) {
      System.out.println("Waiting for node activation: " + nodeThread.getState());
      Thread.sleep(10);
    }

    testSink.waitForResultCount(1, 3000);
    junit.framework.Assert.assertTrue("tuple emmitted", testSink.collectedTuples.size() > 0);

//      while( testSink.collectedTuples.size() < testNum*3 )
//        Thread.sleep(1);
//      logger.debug("tuples size:"+testSink.collectedTuples.size());

      junit.framework.Assert.assertEquals("emitted value for testNum was ", testNum*3, testSink.collectedTuples.size());
      for( int i=0; i<testSink.collectedTuples.size(); i++ ) {
        String str = (String)testSink.collectedTuples.get(i);
        int eq = str.indexOf('=');
        String key = str.substring(1, eq);
        Integer value = Integer.parseInt(str.substring(eq+1, str.length()-1));
        if(key.equals("a")) {
            junit.framework.Assert.assertEquals("emitted value for 'a' was ", new Integer(2), value);
        }
        else if(key.equals("b")) {
            junit.framework.Assert.assertEquals("emitted value for 'b' was ", new Integer(20), value);
        }
        if(key.equals("c")) {
            junit.framework.Assert.assertEquals("emitted value for 'c' was ", new Integer(1000), value);
        }
      }
     long end = System.currentTimeMillis();
//     logger.debug("execution time:"+(end-begin)+" ms");
     //node.teardown();
      logger.debug("end of test sent "+testSink.collectedTuples.size()+ " messages");

  }
/*
    @Test
    public void testProcess() throws InterruptedException {
//      ModuleConfiguration conf = new ModuleConfiguration("subscriber", new HashMap<String, String>());
      Sink sink = node.connect(InputZeroMQStream.OUTPUT, node);
      node.setup(config);

      final AtomicBoolean inactive = new AtomicBoolean(true);
      new Thread()
      {
        @Override
        public void run()
        {
          node.activate(new ModuleContext("ZeroMQInputTestNode", this));
          inactive.set(false);
        }
      }.start();

      int sleeptimes = 0;
      try {
        do {
          Thread.sleep(20);
          sleeptimes++;
          if (sleeptimes > 20) {
            break;
          }
        }
        while (inactive.get());
      }
      catch (InterruptedException ex) {
        logger.debug(ex.getLocalizedMessage());
      }

      OutputZeroMQStream publisher = new OutputZeroMQStream();
      ModuleConfiguration conf1 = new ModuleConfiguration("publisher", new HashMap<String, String>());
      publisher.setup(conf1);

      long begin = System.currentTimeMillis();
      PublisherThread pubThr = new PublisherThread();
      pubThr.setPublisher(publisher);
      pubThr.start();
      pubThr.join();

      while( node.count < testNum*3 )
        Thread.sleep(1);
      junit.framework.Assert.assertEquals("emitted value for testNum was ", testNum*3, node.count);
      for (Map.Entry<String, Integer> e: node.dataMap.entrySet()) {
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
//     logger.debug("execution time:"+(end-begin)+" ms");
     //node.teardown();
      logger.debug("end of test sent "+node.count+ " messages");
    }
    */
}
