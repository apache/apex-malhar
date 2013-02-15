/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.kestrel;

import com.malhartech.api.*;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.stram.StramLocalCluster;
import com.malhartech.util.CircularBuffer;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class KestrelOutputOperatorTest
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(KestrelOutputOperatorTest.class);

  private static final class TestKestrelOutputOperator extends AbstractSinglePortKestrelOutputOperator<String>
  {
    @Override
    public void processTuple(String tuple)
    {
        if (mcc.set(queueName, tuple.getBytes()) == false) {
          logger.debug("Set message:" + tuple + " Error!");
        }
    }
  }

  public class KestrelMessageReceiver
  {
    public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    public int count = 0;
    String queueName = "testQ";
    private SockIOPool pool;
    String[] servers = {"localhost:22133"};
    MemcachedClient mcc;

    public void setup()
    {
      pool = SockIOPool.getInstance();
      pool.setServers(servers);
      pool.setFailover(true);
      pool.setInitConn(10);
      pool.setMinConn(5);
      pool.setMaxConn(250);
      pool.setMaintSleep(30);
      pool.setNagle(false);
      pool.setSocketTO(3000);
      pool.setAliveCheck(true);
      pool.initialize();

      mcc = new MemcachedClient();
      GetQueueThread gqt = new GetQueueThread();
      gqt.start();
    }

    private class GetQueueThread extends Thread
    {
      @Override
      public void run()
      {
        while (true) {
          byte[] result = (byte[])mcc.get(queueName);
          if (result != null) {
            String str = new String(result);
            if (str.indexOf("{") == -1) {
              return;
            }
            int eq = str.indexOf('=');
            String key = str.substring(1, eq);
            int value = Integer.parseInt(str.substring(eq + 1, str.length() - 1));
            dataMap.put(key, value);
            count++;

          }
//        try {
//          Thread.sleep(10);
//        }
//        catch (InterruptedException ex) {
//          logger.debug(ex.toString());
//        }
        }
      }
    }

    public void teardown()
    {
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
    TestKestrelOutputOperator producer = dag.addOperator("producer", new TestKestrelOutputOperator());
    producer.setQueueName("testQ");
    String[] servers = {"localhost:22133"};
    producer.setServers(servers);
    dag.addStream("Stream", source.outPort, producer.inputPort).setInline(true);

    KestrelMessageReceiver consumer = new KestrelMessageReceiver();
    consumer.setup();

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

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

    junit.framework.Assert.assertEquals("emitted value for testNum was ", testNum * 3, consumer.count);
    for (Map.Entry<String, Integer> e: consumer.dataMap.entrySet()) {
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
