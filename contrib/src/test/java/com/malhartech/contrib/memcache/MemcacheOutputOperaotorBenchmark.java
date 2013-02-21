/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.memcache;

import com.malhartech.api.*;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.contrib.memcache.MemcacheOutputOperatorTest.MemcacheMessageReceiver.GetDataThread;
import static com.malhartech.contrib.memcache.MemcacheOutputOperatorTest.SourceModule.holdingBuffer;
import static com.malhartech.contrib.memcache.MemcacheOutputOperatorTest.sentTuples;
import com.malhartech.stram.StramLocalCluster;
import com.malhartech.util.CircularBuffer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MemcacheOutputOperaotorBenchmark
{
  private static final Logger logger = LoggerFactory.getLogger(MemcacheOutputOperaotorBenchmark.class);
  private static HashMap<String, Integer> sendingData = new HashMap<String, Integer>();
  static int sentTuples = 0;
  final static int testNum = 3000;
  final static int totalTuples = testNum*3;

  private static final class TestMemcacheOutputOperator extends AbstractSinglePortMemcacheOutputOperator<HashMap<String, Integer>>
  {
    @Override
    public void processTuple(HashMap<String, Integer> tuple)
    {
      int exp = 60*60*24*30;
      Iterator it = tuple.entrySet().iterator();
      Entry<String, Integer> entry = (Entry)it.next();

      OperationFuture<Boolean> of = client.set(entry.getKey(), exp, entry.getValue());
      try {
        if( of.get() == true )
        ++sentTuples;
      }
      catch (InterruptedException ex) {
        logger.debug(ex.toString());
      }
      catch (ExecutionException ex) {
        logger.debug(ex.toString());
      }
    }
  }

  public class MemcacheMessageReceiver
  {
    MemcachedClient client;
  ArrayList<String> servers = new ArrayList<String>();
    public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    public int count = 0;
    public GetDataThread gdt;

    public void addSever(String server) {
      servers.add(server);
    }
    public void setup() {
      try {
        client = new MemcachedClient(AddrUtil.getAddresses(servers));
      }
      catch (IOException ex) {
        System.out.println(ex.toString());
      }
      gdt = new GetDataThread();
      gdt.start();
    }

    public class GetDataThread extends Thread {
      public volatile boolean working;
      @Override
      public void run() {
        working = true;
        while( working ) {
          if( sentTuples < totalTuples ) {
            try {
              Thread.sleep(10);
            }
            catch (InterruptedException ex) {
              logger.debug(ex.toString());
            }
            continue;
          }
          for(  Entry<String, Integer> e: sendingData.entrySet() ) {
            String key = e.getKey();
            Object value = client.get(key);
            if( value == null ) {
              System.err.println("Exception get null value!!!!!");
              working = false;
            }
            else {
              Integer i = (Integer)value;
              dataMap.put(key, i);
              count++;
              if( count == 3 )
                working = false;
            }
          }
        }
        System.out.println("sending Data:"+sendingData.toString());
      }
    }
  }

  public static class SourceModule extends BaseOperator
  implements InputOperator, ActivationListener<OperatorContext>
  {
    public final transient DefaultOutputPort<HashMap<String, Integer>> outPort = new DefaultOutputPort<HashMap<String, Integer>>(this);
    static transient CircularBuffer<HashMap<String, Integer>> holdingBuffer;
    int testNum;

    @Override
    public void setup(OperatorContext context)
    {
      holdingBuffer = new CircularBuffer<HashMap<String, Integer>>(1024 * 1024);
    }

    public void emitTuple(HashMap<String, Integer> message)
    {
      outPort.emit(message);
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
      sendingData.put("a",2);
      sendingData.put("b",20);
      sendingData.put("c",1000);

      for( int i=0; i<testNum; i++ ) {
        for( Entry<String, Integer> e : sendingData.entrySet() ) {
          HashMap<String, Integer> map  = new HashMap<String, Integer>();
          map.put(e.getKey(), e.getValue());
          holdingBuffer.add(map);
        }
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
  public void testDag() throws Exception {
    String server = "localhost:11211";

    DAG dag = new DAG();
    SourceModule source = dag.addOperator("source", SourceModule.class);
    source.setTestNum(testNum);
    TestMemcacheOutputOperator producer = dag.addOperator("producer", new TestMemcacheOutputOperator());
    producer.addServer(server);
    dag.addStream("Stream", source.outPort, producer.inputPort).setInline(true);

    final MemcacheMessageReceiver consumer = new MemcacheMessageReceiver();
    consumer.addSever(server);
    consumer.setup();

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          while( consumer.gdt.working == true ) {
            Thread.sleep(100);
//            System.out.println("receiver count:"+consumer.count);
          }
        }
        catch (InterruptedException ex) {
        }
        lc.shutdown();
      }
    }.start();

    lc.run();

    System.out.println("consumer count:"+sentTuples);
    junit.framework.Assert.assertEquals("emitted value for testNum was ", sentTuples, totalTuples);
    junit.framework.Assert.assertEquals("emitted value for testNum was ", consumer.count, 3);
//    for (Map.Entry<String, Integer> e: consumer.dataMap.entrySet()) {
    for (Map.Entry<String, Integer> e: sendingData.entrySet()) {
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
