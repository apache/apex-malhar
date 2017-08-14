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
package org.apache.apex.malhar.contrib.redis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;

import redis.clients.jedis.ScanParams;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;

public class RedisInputOperatorTest
{
  private RedisStore operatorStore;
  private RedisStore testStore;

  public static class CollectorModule extends BaseOperator
  {
    static volatile List<KeyValPair<String, String>> resultMap = new ArrayList<KeyValPair<String, String>>();
    static long resultCount = 0;

    public final transient DefaultInputPort<KeyValPair<String, String>> inputPort = new DefaultInputPort<KeyValPair<String, String>>()
    {
      @Override
      public void process(KeyValPair<String, String> tuple)
      {
        resultMap.add(tuple);
        resultCount++;
      }
    };
  }

  @Test
  public void testIntputOperator() throws IOException
  {
    this.operatorStore = new RedisStore();
    this.testStore = new RedisStore();

    testStore.connect();
    ScanParams params = new ScanParams();
    params.count(1);

    testStore.put("test_abc", "789");
    testStore.put("test_def", "456");
    testStore.put("test_ghi", "123");

    try {
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();

      RedisKeyValueInputOperator inputOperator = dag.addOperator("input", new RedisKeyValueInputOperator());
      final CollectorModule collector = dag.addOperator("collector", new CollectorModule());

      inputOperator.setStore(operatorStore);
      dag.addStream("stream", inputOperator.outputPort, collector.inputPort);
      final LocalMode.Controller lc = lma.getController();

      new Thread("LocalClusterController")
      {
        @Override
        public void run()
        {
          long startTms = System.currentTimeMillis();
          long timeout = 50000L;
          try {
            Thread.sleep(1000);
            while (System.currentTimeMillis() - startTms < timeout) {
              if (CollectorModule.resultMap.size() < 3) {
                Thread.sleep(10);
              } else {
                break;
              }
            }
          } catch (InterruptedException ex) {
            //
          }
          lc.shutdown();
        }
      }.start();

      lc.run();

      Assert.assertTrue(CollectorModule.resultMap.contains(new KeyValPair<String, String>("test_abc", "789")));
      Assert.assertTrue(CollectorModule.resultMap.contains(new KeyValPair<String, String>("test_def", "456")));
      Assert.assertTrue(CollectorModule.resultMap.contains(new KeyValPair<String, String>("test_ghi", "123")));
    } finally {
      for (KeyValPair<String, String> entry : CollectorModule.resultMap) {
        testStore.remove(entry.getKey());
      }
      testStore.disconnect();
    }
  }

  @Test
  public void testRecoveryAndIdempotency() throws Exception
  {
    this.operatorStore = new RedisStore();
    this.testStore = new RedisStore();

    testStore.connect();
    ScanParams params = new ScanParams();
    params.count(1);

    testStore.put("test_abc", "789");
    testStore.put("test_def", "456");
    testStore.put("test_ghi", "123");

    RedisKeyValueInputOperator operator = new RedisKeyValueInputOperator();
    operator.setWindowDataManager(new FSWindowDataManager());

    operator.setStore(operatorStore);
    operator.setScanCount(1);
    Attribute.AttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();

    operator.outputPort.setSink(sink);
    OperatorContext context = mockOperatorContext(1, attributeMap);

    try {
      operator.setup(context);
      operator.beginWindow(1);
      operator.emitTuples();
      operator.endWindow();

      int numberOfMessagesInWindow1 = sink.collectedTuples.size();
      sink.collectedTuples.clear();

      operator.beginWindow(2);
      operator.emitTuples();
      operator.endWindow();
      int numberOfMessagesInWindow2 = sink.collectedTuples.size();
      sink.collectedTuples.clear();

      // failure and then re-deployment of operator
      // Re-instantiating to reset values
      operator = new RedisKeyValueInputOperator();
      operator.setWindowDataManager(new FSWindowDataManager());
      operator.setStore(operatorStore);
      operator.setScanCount(1);
      operator.outputPort.setSink(sink);
      operator.setup(context);

      Assert.assertEquals("largest recovery window", 2, operator.getWindowDataManager().getLargestCompletedWindow());

      operator.beginWindow(1);
      operator.emitTuples();
      operator.emitTuples();
      operator.endWindow();

      Assert.assertEquals("num of messages in window 1", numberOfMessagesInWindow1, sink.collectedTuples.size());

      sink.collectedTuples.clear();
      operator.beginWindow(2);
      operator.emitTuples();
      operator.endWindow();
      Assert.assertEquals("num of messages in window 2",numberOfMessagesInWindow2, sink.collectedTuples.size());
    } finally {
      for (Object e : sink.collectedTuples) {
        KeyValPair<String, String> entry = (KeyValPair<String, String>)e;
        testStore.remove(entry.getKey());
      }
      sink.collectedTuples.clear();
      operator.getWindowDataManager().committed(5);
      operator.teardown();
    }
  }
}
