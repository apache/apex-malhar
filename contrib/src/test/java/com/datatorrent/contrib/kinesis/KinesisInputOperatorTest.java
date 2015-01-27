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
package com.datatorrent.contrib.kinesis;

import com.datatorrent.api.*;
import com.datatorrent.api.DAG.Locality;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("myInput", this);
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

  @Override
  @After
  public void afterTest()
  {
    collections.clear();
    super.afterTest();
  }
}