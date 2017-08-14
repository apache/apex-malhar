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

import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.helper.CollectorModule;
import org.apache.apex.malhar.contrib.helper.MessageQueueTestHelper;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.LocalMode;

import com.datatorrent.netlet.util.DTThrowable;

/**
 *
 */
public class ZeroMQInputOperatorTest
{
  protected static Logger logger = LoggerFactory.getLogger(ZeroMQInputOperatorTest.class);

  @Test
  public void testDag() throws InterruptedException, Exception
  {
    final int testNum = 3;
    testHelper(testNum);
  }

  protected void testHelper(final int testNum)
  {
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    final ZeroMQMessageGenerator publisher = new ZeroMQMessageGenerator();
    publisher.setup();

    ZeroMQInputOperator generator = dag.addOperator("Generator", ZeroMQInputOperator.class);
    final CollectorModule<byte[]> collector = dag.addOperator("Collector", new CollectorModule<byte[]>());

    generator.setFilter("");
    generator.setUrl("tcp://localhost:5556");
    generator.setSyncUrl("tcp://localhost:5557");

    dag.addStream("Stream", generator.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);
    new Thread()
    {
      @Override
      public void run()
      {
        try {
          publisher.generateMessages(testNum);
        } catch (InterruptedException ex) {
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
        long startTms = System.currentTimeMillis();
        long timeout = 10000L;
        try {
          while (!collector.inputPort.collections.containsKey("collector") && System.currentTimeMillis() - startTms < timeout) {
            Thread.sleep(500);
          }
          Thread.sleep(1000);
          startTms = System.currentTimeMillis();
          while (System.currentTimeMillis() - startTms < timeout) {
            List<?> list = collector.inputPort.collections.get("collector");
            if (list.size() < testNum * 3) {
              Thread.sleep(10);
            } else {
              break;
            }
          }
        } catch (InterruptedException ex) {
          DTThrowable.rethrow(ex);
        } finally {
          logger.debug("Shutting down..");
          lc.shutdown();
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            DTThrowable.rethrow(e);
          } finally {
            publisher.teardown();
          }
        }
      }
    }.start();

    lc.run();


    // logger.debug("collection size:"+collector.inputPort.collections.size()+" "+collector.inputPort.collections.toString());

    MessageQueueTestHelper.validateResults(testNum, collector.inputPort.collections);
    logger.debug("end of test");
  }
}

