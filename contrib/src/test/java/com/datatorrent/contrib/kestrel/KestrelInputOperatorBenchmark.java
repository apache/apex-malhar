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
import com.datatorrent.contrib.kestrel.AbstractSinglePortKestrelInputOperator;
import com.datatorrent.contrib.kestrel.MemcachedClient;
import com.datatorrent.contrib.kestrel.SockIOPool;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KestrelInputOperatorBenchmark
{
  private static Logger logger = LoggerFactory.getLogger(KestrelInputOperatorTest.class);
  static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();

  public static final class TestStringKestrelInputOperator extends AbstractSinglePortKestrelInputOperator<String>
  {
    @Override
    public String getTuple(byte[] message) {
      return new String(message);
    }

    public void replayTuples(long windowId)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  private final class KestrelMessageGenerator
  {
    public String queueName = "testQ";
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
      mcc.flush(queueName,null);

    }

    public void setQueueName(String queueName)
    {
      this.queueName = queueName;
    }

    public void send(Object message)
    {
      String msg = message.toString();
      if (mcc.set(queueName, msg.getBytes()) == false) {
        logger.debug("Set message:" + msg + " Error!");
      }
    }

    public void teardown()
    {
      pool.shutDown();
    }

    public void generateMessages(int msgCount) throws InterruptedException
    {
      for (int i = 0; i < msgCount; i++) {
        HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
        dataMapa.put("a", 2);
        send(dataMapa);

        HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
        dataMapb.put("b", 20);
        send(dataMapb);

        HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
        dataMapc.put("c", 1000);
        send(dataMapc);
      }
    }
  }

  public static class CollectorInputPort<T> extends DefaultInputPort<T>
  {
    ArrayList<T> list;
    final String id;

    public CollectorInputPort(String id, Operator module)
    {
      super();
      this.id = id;
      collections.put(id, list = new ArrayList<T>());
    }

    @Override
    public void process(T tuple)
    {
//      System.out.print("collector process:" + tuple);
      list.add(tuple);
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
//        collections.put(id, list = new ArrayList<T>());
      }
    }
  }

  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("collector", this);
  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testBechmark() throws Exception
  {
    final int testNum = 20000;
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    TestStringKestrelInputOperator consumer = dag.addOperator("Generator", TestStringKestrelInputOperator.class);
    CollectorModule<String> collector = dag.addOperator("Collector", new CollectorModule<String>());
    String[] servers = {"localhost:22133"};
    consumer.setServers(servers);
    consumer.setQueueName("testQ");

    new Thread()
    {
      @Override
      public void run()
      {
        KestrelMessageGenerator producer = new KestrelMessageGenerator();
        producer.setQueueName("testQ");
        producer.setup();
        try {
          producer.generateMessages(testNum);
        }
        catch (InterruptedException ex) {
          logger.debug(ex.toString());
        }
      }
    }.start();

    dag.addStream("Stream", consumer.outputPort, collector.inputPort).setInline(true);

    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    try {
      while (true) {
        ArrayList<String> strList = (ArrayList<String>)collections.get("collector");
        if (testNum * 3 > strList.size()) {
          Thread.sleep(10);
          if( strList.size() % 1000 == 0)
            logger.debug("processed "+strList.size()+" tuples");
        }
        else {
          break;
        }
      }
    }
    catch (InterruptedException ex) {
    }
    lc.shutdown();

    logger.debug("collection size:" + collections.size() + " " + collections.toString());

    ArrayList<String> strList = (ArrayList<String>)collections.get("collector");
    Assert.assertEquals("emitted value for testNum was ", testNum * 3, strList.size());
    for (int i = 0; i < strList.size(); i++) {
      String str = strList.get(i);
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
    logger.debug(String.format("\nBenchmarked %d tuples", testNum * 3));
    logger.debug("end of test");
  }
}
