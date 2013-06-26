/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.kestrel;

import com.datatorrent.api.*;
import com.datatorrent.contrib.kestrel.AbstractSinglePortKestrelOutputOperator;
import com.datatorrent.contrib.kestrel.MemcachedClient;
import com.datatorrent.contrib.kestrel.SockIOPool;
import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Context.OperatorContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 *
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
    public final transient DefaultOutputPort<String> outPort = new DefaultOutputPort<String>();
    transient ArrayBlockingQueue<byte[]> holdingBuffer;
    int testNum;

    @Override
    public void setup(OperatorContext context)
    {
      holdingBuffer = new ArrayBlockingQueue<byte[]>(1024 * 1024);
    }

    public void emitTuple(byte[] message)
    {
      outPort.emit(new String(message));
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
    TestKestrelOutputOperator producer = dag.addOperator("producer", new TestKestrelOutputOperator());
    producer.setQueueName("testQ");
    String[] servers = {"localhost:22133"};
    producer.setServers(servers);
    dag.addStream("Stream", source.outPort, producer.inputPort).setInline(true);

    KestrelMessageReceiver consumer = new KestrelMessageReceiver();
    consumer.setup();

    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    try {
      Thread.sleep(1000);
    }
    catch (InterruptedException ex) {
    }
    lc.shutdown();

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
