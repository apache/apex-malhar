/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.memcache_whalin;

import com.datatorrent.contrib.memcache_whalin.AbstractSinglePortMemcacheOutputOperator;
import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Context.OperatorContext;
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MemcacheOutputOperatorTest
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(MemcacheOutputOperatorTest.class);
  private static HashMap<String, Integer> sendingData = new HashMap<String, Integer>();
  static int sentTuples = 0;
  final static int totalTuples = 9;

  private static final class TestMemcacheOutputOperator extends AbstractSinglePortMemcacheOutputOperator<HashMap<String, Integer>>
  {
    @Override
    public void processTuple(HashMap<String, Integer> tuple)
    {
      Iterator it = tuple.entrySet().iterator();
      Entry entry = (Entry)it.next();

      if (mcc.set((String)entry.getKey(),entry.getValue()) == false) {
        logger.debug("Set message:" + tuple + " Error!");
      }
      else {
        ++sentTuples;
      }
    }
  }

  public class MemcacheMessageReceiver
  {
    public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    public int count = 0;
    private SockIOPool pool;
    String[] servers = {"localhost:11211"};
    MemCachedClient mcc;

    public void setMcc(MemCachedClient mcc) {
      this.mcc = mcc;
    }

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

      mcc = new MemCachedClient();
      GetDataThread gdt = new GetDataThread();
      gdt.start();
    }

    private class GetDataThread extends Thread
    {
      @Override
      public void run()
      {
        boolean working = true;
        while( working ) {
          if( sentTuples != totalTuples ) {
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
            Object value = mcc.get(key);
            if( value == null ) {
              System.err.println("Exception get null value!!!!!");
              working = false;
            }
            else {
              Integer i = (Integer)value;
              dataMap.put(key, i);
              count++;
              if( count == totalTuples )
                working = false;
            }
          }
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
    public final transient DefaultOutputPort<HashMap<String, Integer>> outPort = new DefaultOutputPort<HashMap<String, Integer>>();
    static transient ArrayBlockingQueue<HashMap<String, Integer>> holdingBuffer;
    int testNum;

    @Override
    public void setup(OperatorContext context)
    {
      holdingBuffer = new ArrayBlockingQueue<HashMap<String, Integer>>(1024 * 1024);
    }

    public void emitTuple(HashMap<String, Integer> message)
    {
      outPort.emit(message);
    }

    @Override
    public void emitTuples()
    {
      for (int i = holdingBuffer.size(); i-- > 0;) {
        emitTuple(holdingBuffer.poll());
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

    @Override
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

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    SourceModule source = dag.addOperator("source", SourceModule.class);
    source.setTestNum(testNum);
    TestMemcacheOutputOperator producer = dag.addOperator("producer", new TestMemcacheOutputOperator());
    String[] servers = {"localhost:11211"};
    producer.setServers(servers);
    dag.addStream("Stream", source.outPort, producer.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    MemcacheMessageReceiver consumer = new MemcacheMessageReceiver();
    consumer.setup();

    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    try {
      Thread.sleep(2000);
    }
    catch (InterruptedException ex) {
    }
    lc.shutdown();

    System.out.println("consumer count:"+consumer.count);
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
