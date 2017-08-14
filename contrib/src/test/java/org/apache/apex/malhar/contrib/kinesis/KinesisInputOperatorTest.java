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
package org.apache.apex.malhar.contrib.kinesis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.wal.WindowDataManager;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class KinesisInputOperatorTest extends KinesisOperatorTestBase
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KinesisInputOperatorTest.class);
  static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();
  static AtomicInteger tupleCount = new AtomicInteger();
  static CountDownLatch latch;

  /**
   * Test Operator to collect tuples from KinesisSingleInputStringOperator.
   *
   * @param <T>
   */
  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("myInput");
  }

  public static class CollectorInputPort<T> extends DefaultInputPort<T>
  {
    ArrayList<T> list;
    final String id;

    public CollectorInputPort(String id)
    {
      super();
      this.id = id;
    }

    @Override
    public void process(T tuple)
    {
      if (tuple.equals(KinesisOperatorTestBase.END_TUPLE)) {
        if (latch != null) {
          latch.countDown();
        }
        return;
      }
      list.add(tuple);
      tupleCount.incrementAndGet();
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        collections.put(id, list = new ArrayList<T>());
      }
    }
  }

  @Test
  public void testWindowDataManager() throws Exception
  {
    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    KinesisStringInputOperator inputOperator = dag.addOperator("KinesisInput", new KinesisStringInputOperator()
    {
      @Override
      public void deactivate()
      {
      }

      @Override
      public void teardown()
      {
      }
    });
    testMeta.operator = inputOperator;
    Assert.assertTrue("Default behaviour of WindowDataManager changed",
        (inputOperator.getWindowDataManager() instanceof WindowDataManager.NoopWindowDataManager));
  }

  /**
   * Test AbstractKinesisSinglePortInputOperator (i.e. an input adapter for
   * Kinesis, consumer). This module receives data from an outside test
   * generator through Kinesis message bus and feed that data into Malhar
   * streaming platform.
   *
   * [Generate message and send that to Kinesis message bus] ==> [Receive that
   * message through Kinesis input adapter(i.e. consumer) and send using
   * emitTuples() interface on output port during onMessage call]
   *
   *
   * @throws Exception
   */
  @Test
  public void testKinesisInputOperator() throws Exception
  {
    int totalCount = 100;
    // initial the latch for this test
    latch = new CountDownLatch(1);

    // Start producer
    KinesisTestProducer p = new KinesisTestProducer(streamName);
    p.setSendCount(totalCount);
    p.setBatchSize(500);
    new Thread(p).start();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    // Create KinesisSinglePortStringInputOperator
    KinesisStringInputOperator node = dag.addOperator("Kinesis message consumer", KinesisStringInputOperator.class);
    node.setAccessKey(credentials.getCredentials().getAWSSecretKey());
    node.setSecretKey(credentials.getCredentials().getAWSAccessKeyId());
    KinesisConsumer consumer = new KinesisConsumer();
    consumer.setStreamName(streamName);
    consumer.setRecordsLimit(totalCount);
    node.setConsumer(consumer);

    // Create Test tuple collector
    CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("Kinesis message", node.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.runAsync();

    // Wait 45s for consumer finish consuming all the messages
    latch.await(45000, TimeUnit.MILLISECONDS);

    // Check results
    Assert.assertEquals("Collections size", 1, collections.size());
    Assert.assertEquals("Tuple count", totalCount, collections.get(collector.inputPort.id).size());
    logger.debug(String.format("Number of emitted tuples: %d", collections.get(collector.inputPort.id).size()));

    lc.shutdown();
  }

  @Test
  public void testKinesisByteArrayInputOperator() throws Exception
  {
    int totalCount = 10;
    // initial the latch for this test
    latch = new CountDownLatch(1);

    // Start producer
    KinesisTestProducer p = new KinesisTestProducer(streamName);
    p.setSendCount(totalCount);
    p.setBatchSize(9);
    new Thread(p).start();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    // Create KinesisByteArrayInputOperator and set some properties with respect to consumer.
    KinesisByteArrayInputOperator node = dag.addOperator("Kinesis message consumer", KinesisByteArrayInputOperator.class);
    node.setAccessKey(credentials.getCredentials().getAWSSecretKey());
    node.setSecretKey(credentials.getCredentials().getAWSAccessKeyId());
    KinesisConsumer consumer = new KinesisConsumer();
    consumer.setStreamName(streamName);
    consumer.setRecordsLimit(totalCount);
    node.setConsumer(consumer);

    // Create Test tuple collector
    CollectorModule<byte[]> collector = dag.addOperator("TestMessageCollector", new CollectorModule<byte[]>());

    // Connect ports
    dag.addStream("Kinesis message", node.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.runAsync();

    // Wait 45s for consumer finish consuming all the messages
    latch.await(45000, TimeUnit.MILLISECONDS);

    // Check results
    Assert.assertEquals("Collections size", 1, collections.size());
    Assert.assertEquals("Tuple count", totalCount, collections.get(collector.inputPort.id).size());
    logger.debug(String.format("Number of emitted tuples: %d", collections.get(collector.inputPort.id).size()));

    lc.shutdown();
  }

  @Override
  @After
  public void afterTest()
  {
    collections.clear();
    super.afterTest();
  }

  public static class TestMeta extends TestWatcher
  {
    String baseDir;
    KinesisStringInputOperator operator;
    CollectorTestSink<Object> sink;
    Context.OperatorContext context;

    @Override
    protected void starting(Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      baseDir = "target/" + className + "/" + methodName;
    }

    @Override
    protected void finished(Description description)
    {
      operator.deactivate();
      operator.teardown();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testRecoveryAndIdempotency() throws Exception
  {
    int totalCount = 10;

    // initial the latch for this test
    latch = new CountDownLatch(1);

    // Start producer
    KinesisTestProducer p = new KinesisTestProducer(streamName);
    p.setSendCount(totalCount);
    p.setBatchSize(500);
    new Thread(p).start();

    Attribute.AttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(Context.OperatorContext.SPIN_MILLIS, 500);
    attributeMap.put(Context.DAGContext.APPLICATION_PATH, testMeta.baseDir);

    testMeta.context = mockOperatorContext(1, attributeMap);
    testMeta.operator = new KinesisStringInputOperator();

    KinesisUtil.getInstance().setClient(client);

    KinesisConsumer consumer = new KinesisConsumer();
    consumer.setStreamName(streamName);
    consumer.setInitialOffset("earliest");
    testMeta.operator.setConsumer(consumer);

    testMeta.sink = new CollectorTestSink<Object>();
    testMeta.operator.outputPort.setSink(testMeta.sink);
    latch.await(4000, TimeUnit.MILLISECONDS);
    testMeta.operator.setup(testMeta.context);

    testMeta.operator.activate(testMeta.context);
    latch.await(4000, TimeUnit.MILLISECONDS);
    testMeta.operator.beginWindow(1);
    testMeta.operator.emitTuples();
    testMeta.operator.endWindow();

    latch.await(4000, TimeUnit.MILLISECONDS);
    //failure and then re-deployment of operator
    testMeta.sink.collectedTuples.clear();
    testMeta.operator.setup(testMeta.context);
    testMeta.operator.activate(testMeta.context);

    Assert.assertEquals("largest recovery window", 1, testMeta.operator.getWindowDataManager().getLargestCompletedWindow());

    testMeta.operator.beginWindow(1);
    testMeta.operator.endWindow();
    Assert.assertEquals("num of messages in window 1", 10, testMeta.sink.collectedTuples.size());
    testMeta.sink.collectedTuples.clear();
  }
}
