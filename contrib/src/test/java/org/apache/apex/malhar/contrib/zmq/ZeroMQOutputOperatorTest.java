/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.zmq;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.helper.SourceModule;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.LocalMode;

import com.datatorrent.netlet.util.DTThrowable;

public class ZeroMQOutputOperatorTest
{
  protected static org.slf4j.Logger logger = LoggerFactory.getLogger(ZeroMQOutputOperatorTest.class);

  @Test
  public void testDag() throws Exception
  {
    final int testNum = 3;

    runTest(testNum);

    logger.debug("end of test");
  }

  protected void runTest(final int testNum)
  {
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    SourceModule source = dag.addOperator("source", new SourceModule());
    source.setTestNum(testNum);
    final ZeroMQOutputOperator collector = dag.addOperator("generator", new ZeroMQOutputOperator());
    collector.setUrl("tcp://*:5556");
    collector.setSyncUrl("tcp://*:5557");
    collector.setSUBSCRIBERS_EXPECTED(1);

    dag.addStream("Stream", source.outPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    final ZeroMQMessageReceiver receiver = new ZeroMQMessageReceiver();
    receiver.setup();
    final Thread t = new Thread(receiver);
    t.start();
    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(1000);
          long timeout = 10000L;
          long startTms = System.currentTimeMillis();
          while (System.currentTimeMillis() - startTms < timeout) {
            if (receiver.count < testNum * 3) {
              Thread.sleep(10);
            } else {
              break;
            }
          }
        } catch (InterruptedException ex) {
          DTThrowable.rethrow(ex);
        } finally {
          logger.debug("done...");
          lc.shutdown();
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            DTThrowable.rethrow(e);
          } finally {
            t.interrupt();
            receiver.teardown();
          }
        }
      }
    }.start();

    lc.run();

    Assert.assertEquals("emitted value for testNum was ", testNum * 3, receiver.count);
    for (Map.Entry<String, Integer> e : receiver.dataMap.entrySet()) {
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Integer(2), e.getValue());
      } else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted value for 'b' was ", new Integer(20), e.getValue());
      } else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted value for 'c' was ", new Integer(1000), e.getValue());
      }
    }
  }
}
