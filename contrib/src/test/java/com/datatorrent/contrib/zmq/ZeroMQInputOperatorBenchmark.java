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
package com.datatorrent.contrib.zmq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;

/**
 *
 */
public class ZeroMQInputOperatorBenchmark
{
  String pubAddr = "tcp://*:5556";
  String syncAddr = "tcp://*:5557";
  private static Logger logger = LoggerFactory.getLogger(ZeroMQInputOperatorTest.class);
  static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();

  public static final class TestStringZeroMQInputOperator extends AbstractSinglePortZeroMQInputOperator<String>
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

  private final class ZeroMQMessageGenerator
  {
    private ZMQ.Context context;
    private ZMQ.Socket publisher;
    private ZMQ.Socket syncservice;
    private final int SUBSCRIBERS_EXPECTED = 1;

    public void setup()
    {
      context = ZMQ.context(1);
      logger.debug("Publishing on ZeroMQ");
      publisher = context.socket(ZMQ.PUB);
      publisher.bind(pubAddr);
      syncservice = context.socket(ZMQ.REP);
      syncservice.bind(syncAddr);
    }

    public void send(Object message)
    {
      String msg = message.toString();
      // logger.debug("publish:"+msg);
      publisher.send(msg.getBytes(), 0);
    }

    public void teardown()
    {
      publisher.close();
      context.term();
    }

    public void generateMessages(int msgCount) throws InterruptedException
    {
      for (int subscribers = 0; subscribers < SUBSCRIBERS_EXPECTED; subscribers++) {
        byte[] value = syncservice.recv(0);
        syncservice.send("".getBytes(), 0);
      }
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
    }

    @Override
    public void process(T tuple)
    {
//      System.out.print("collector process:"+tuple);
      list.add(tuple);
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        collections.put(id, list = new ArrayList<T>());
      }
    }
  }

  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("collector", this);
  }

  @Test
  public void testDag() throws InterruptedException, Exception {
      final int testNum = 2000000;
    final ZeroMQMessageGenerator publisher = new ZeroMQMessageGenerator();
    publisher.setup();

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    TestStringZeroMQInputOperator generator = dag.addOperator("Generator", TestStringZeroMQInputOperator.class);
    CollectorModule<String> collector = dag.addOperator("Collector", new CollectorModule<String>());

    generator.setFilter("");
    generator.setUrl("tcp://localhost:5556");
    generator.setSyncUrl("tcp://localhost:5557");

    dag.addStream("Stream", generator.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);
    new Thread() {
      @Override
      public void run() {
        try {
          publisher.generateMessages(testNum);
        }
        catch (InterruptedException ex) {
          logger.debug(ex.toString());
        }
      }
    }.start();

    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(1000);
          while (true) {
            ArrayList<String> strList = (ArrayList<String>)collections.get("collector");
            if (strList.size() < testNum * 3) {
              Thread.sleep(10);
            }
            else {
              break;
            }
          }
        }
        catch (InterruptedException ex) {
        }
        lc.shutdown();
      }
    }.start();

    lc.run();

//    logger.debug("collection size:"+collections.size()+" "+collections.toString());

    ArrayList<String> strList =(ArrayList<String>)collections.get("collector");
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
