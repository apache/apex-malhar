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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;

/**
 * A test to verify the input operator will be automatically partitioned per kinesis partition
 * This test is launching its own Kinesis cluster.
 */
public class KinesisPartitionableInputOperatorTest extends KinesisOperatorTestBase
{

  public KinesisPartitionableInputOperatorTest()
  {
    // This class want to initialize several kinesis shards for multiple partitions
    hasMultiPartition = true;
  }

  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KinesisPartitionableInputOperatorTest.class);
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

  /**
   * Test AbstractKinesisSinglePortInputOperator (i.e. an input adapter for
   * Kinesis, aka consumer). This module receives data from an outside test
   * generator through Kinesis and feed that data into Malhar
   * streaming platform.
   *
   * [Generate Records and send that to Kinesis ] ==> [Receive that
   * Records through Kinesis input adapter(i.e. consumer) and send using
   * emitTuples() interface on output port during onRecord call]
   *
   *
   * @throws Exception
   */
  @Test
  public void testPartitionableSimpleConsumerInputOperator() throws Exception
  {
    // Create template simple consumer
    KinesisConsumer consumer = new KinesisConsumer();
    testPartitionableInputOperator(consumer);
  }

  public void testPartitionableInputOperator(KinesisConsumer consumer) throws Exception
  {
    // Set to 2 because we want to make sure END_TUPLE from both 2 partitions are received
    latch = new CountDownLatch(2);

    int totalCount = 100;

    // Start producer
    KinesisTestProducer p = new KinesisTestProducer(streamName, true);
    p.setSendCount(totalCount);
    new Thread(p).start();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    // Create KinesisSinglePortStringInputOperator
    KinesisStringInputOperator node = dag.addOperator("Kinesis consumer", KinesisStringInputOperator.class);
    node.setAccessKey(credentials.getCredentials().getAWSSecretKey());
    node.setSecretKey(credentials.getCredentials().getAWSAccessKeyId());
    node.setStreamName(streamName);
    //set topic
    consumer.setStreamName(streamName);
    node.setConsumer(consumer);

    // Create Test tuple collector
    CollectorModule<String> collector = dag.addOperator("RecordsCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("Kinesis stream", node.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.runAsync();

    //Wait 15s for consumer finish consuming all the records
    latch.await(15000, TimeUnit.MILLISECONDS);

    // Check results
    Assert.assertEquals("Collections size", 1, collections.size());
    Assert.assertEquals("Tuple count", totalCount, collections.get(collector.inputPort.id).size());
    logger.debug(String.format("Number of emitted tuples: %d", collections.get(collector.inputPort.id).size()));

    //p.close();
    lc.shutdown();
  }

}
