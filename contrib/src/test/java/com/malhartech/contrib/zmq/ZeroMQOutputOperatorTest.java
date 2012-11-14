/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.zmq;

import com.malhartech.api.*;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.stram.StramLocalCluster;
import com.malhartech.util.CircularBuffer;
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
    @Override
    public void processTuple(String tuple)
    {
      if (!syncStarted) {
        startSyncJob();
      }
      logger.debug("processTuple:" + tuple);
      publisher.send(tuple.getBytes(), 0);
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
      sendSync();
    }

    public void sendSync()
    {
      syncclient.send("".getBytes(), 0);
    }

    @Override
    public void run()
    {
      logger.debug("receiver running");
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
        logger.debug("\nsubscriber recv:" + str);
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

  public static class SourceModule extends BaseOperator
          implements InputOperator, ActivationListener<OperatorContext>
  {
    public final transient DefaultOutputPort<String> outPort = new DefaultOutputPort<String>(this);
    transient CircularBuffer<byte[]> holdingBuffer;
    int testNum;

    @Override
    public void setup(OperatorContext context)
    {
      holdingBuffer = new CircularBuffer<byte[]>(1024 * 1024);
    }

    public void emitTuple(byte[] message)
    {
      outPort.emit(new String(message));
    }

    @Override
    public void emitTuples()
    {
      for (int i = holdingBuffer.size(); i-- > 0;) {
        emitTuple(holdingBuffer.pollUnsafe());
      }
    }

    @Override
    public void activate(OperatorContext ctx)
    {
      for (int i = 0; i < testNum; i++) {
        HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
        dataMapa.put("a", 2);
        holdingBuffer.add(dataMapa.toString().getBytes());

        HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
        dataMapb.put("b", 20);
        holdingBuffer.add(dataMapb.toString().getBytes());

        HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
        dataMapc.put("c", 1000);
        holdingBuffer.add(dataMapc.toString().getBytes());
      }
    }

    public void setTestNum(int testNum)
    {
      this.testNum = testNum;
    }

    public void deactivate()
    {
    }

    public void replayTuples(long windowId)
    {
    }
  }

  @Test
  public void testDag() throws Exception
  {
    final int testNum = 3;

    DAG dag = new DAG();
    SourceModule source = dag.addOperator("source", SourceModule.class);
    source.setTestNum(testNum);
    final TestStringZeroMQOutputOperator collector = dag.addOperator("generator", new TestStringZeroMQOutputOperator());
    collector.setUrl("tcp://*:5556");
    collector.setSyncUrl("tcp://*:5557");
    collector.setSUBSCRIBERS_EXPECTED(1);

    dag.addStream("Stream", source.outPort, collector.inputPort).setInline(true);

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    ZeroMQMessageReceiver receiver = new ZeroMQMessageReceiver();
    receiver.setup();
    new Thread(receiver).start();

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException ex) {
        }
        lc.shutdown();
      }
    }.start();

    lc.run();

    junit.framework.Assert.assertEquals("emitted value for testNum was ", testNum * 3, receiver.count);
    for (Map.Entry<String, Integer> e : receiver.dataMap.entrySet()) {
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
    logger.debug("end of test");
  }
}
