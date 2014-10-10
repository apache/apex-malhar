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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.ActivationListener;

/**
 *
 */
public class ZeroMQOutputOperatorBenchmark
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(ZeroMQOutputOperatorTest.class);

  private static final class TestStringZeroMQOutputOperator extends AbstractSinglePortZeroMQOutputOperator<String>
  {
    @Override
    public void processTuple(String tuple)
    {
      if (!syncStarted) {
        startSyncJob();
      }
//      logger.debug("processTuple:" + tuple);
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
//        logger.debug("\nsubscriber recv:" + str);
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
    public final transient DefaultOutputPort<String> outPort = new DefaultOutputPort<String>();
    transient ArrayBlockingQueue<byte[]> holdingBuffer;
    int testNum;

    @Override
    public void setup(OperatorContext context)
    {
      holdingBuffer = new ArrayBlockingQueue<byte[]>(10240 * 1024);
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
    final int testNum = 2000000;

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    SourceModule source = dag.addOperator("source", SourceModule.class);
    source.setTestNum(testNum);
    final TestStringZeroMQOutputOperator collector = dag.addOperator("generator", new TestStringZeroMQOutputOperator());
    collector.setUrl("tcp://*:5556");
    collector.setSyncUrl("tcp://*:5557");
    collector.setSUBSCRIBERS_EXPECTED(1);

    dag.addStream("Stream", source.outPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    final ZeroMQMessageReceiver receiver = new ZeroMQMessageReceiver();
    receiver.setup();
    new Thread(receiver).start();

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(1000);
          while (true) {
            if (receiver.count < testNum * 3) {
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

    Assert.assertEquals("emitted value for testNum was ", testNum * 3, receiver.count);
    for (Map.Entry<String, Integer> e : receiver.dataMap.entrySet()) {
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Integer(2), e.getValue());
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted value for 'b' was ", new Integer(20), e.getValue());
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted value for 'c' was ", new Integer(1000), e.getValue());
      }
    }
    logger.debug(String.format("\nBenchmarked %d tuples", testNum * 3));
    logger.debug("end of test");
  }
}
