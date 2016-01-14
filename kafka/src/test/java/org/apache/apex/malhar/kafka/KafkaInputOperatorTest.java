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
package org.apache.apex.malhar.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;

/**
 * A bunch of test to verify the input operator will be automatically partitioned per kafka partition This test is launching its
 * own Kafka cluster.
 */
@RunWith(Parameterized.class)
public class KafkaInputOperatorTest extends KafkaOperatorTestBase
{

  private int totalBrokers = 0;

  private String partition = null;

  @Parameterized.Parameters(name = "multi-cluster: {0}, multi-partition: {1}, partition: {2}")
  public static Collection<Object[]> testScenario()
  {
    return Arrays.asList(new Object[][]{{true, false, "one_to_one"},// multi cluster with single partition
      {true, false, "one_to_many"},
      {true, true, "one_to_one"},// multi cluster with multi partitions
      {true, true, "one_to_many"},
      {false, true, "one_to_one"}, // single cluster with multi partitions
      {false, true, "one_to_many"},
      {false, false, "one_to_one"}, // single cluster with single partitions
      {false, false, "one_to_many"}
    });
  }

  public KafkaInputOperatorTest(boolean hasMultiCluster, boolean hasMultiPartition, String partition)
  {
    // This class want to initialize several kafka brokers for multiple partitions
    this.hasMultiCluster = hasMultiCluster;
    this.hasMultiPartition = hasMultiPartition;
    int cluster = 1 + (hasMultiCluster ? 1 : 0);
    totalBrokers = (1 + (hasMultiPartition ? 1 : 0)) * cluster;
    this.partition = partition;
  }

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaInputOperatorTest.class);
  private static List<String> tupleCollection = new LinkedList<>();
  private static CountDownLatch latch;
  private static boolean hasFailure = false;
  private static int failureTrigger = 3000;
  private static int k = 0;

  /**
   * Test Operator to collect tuples from KafkaSingleInputStringOperator.
   *
   * @param
   */
  public static class CollectorModule extends BaseOperator
  {
    public final transient CollectorInputPort inputPort = new CollectorInputPort();
  }

  public static class CollectorInputPort extends DefaultInputPort<byte[]>
  {

    @Override
    public void process(byte[] bt)
    {
      String tuple = new String(bt);
      if (hasFailure && k++ == failureTrigger) {
        //you can only kill yourself once
        hasFailure = false;
        throw new RuntimeException();
      }
      if (tuple.equals(KafkaOperatorTestBase.END_TUPLE)) {
        if (latch != null) {
          latch.countDown();
        }
        return;
      }
      tupleCollection.add(tuple);
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        tupleCollection.clear();
      }
    }
  }

  /**
   * Test AbstractKafkaSinglePortInputOperator (i.e. an input adapter for Kafka, aka consumer). This module receives
   * data from an outside test generator through Kafka message bus and feed that data into Malhar streaming platform.
   *
   * [Generate message and send that to Kafka message bus] ==> [Receive that message through Kafka input adapter(i.e.
   * consumer) and send using emitTuples() interface on output port]
   *
   *
   * @throws Exception
   */
  @Test
  public void testPartitionableInputOperator() throws Exception
  {
    hasFailure = false;
    testInputOperator(false);
  }


  @Test
  public void testPartitionableInputOperatorWithFailure() throws Exception
  {
    hasFailure = true;
    testInputOperator(true);
  }

  public void testInputOperator(boolean hasFailure) throws Exception
  {

    // each broker should get a END_TUPLE message
    latch = new CountDownLatch(totalBrokers);

    int totalCount = 10000;

    // Start producer
    KafkaTestProducer p = new KafkaTestProducer(TEST_TOPIC, hasMultiPartition, hasMultiCluster);
    p.setSendCount(totalCount);
    Thread t = new Thread(p);
    t.start();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    // Create KafkaSinglePortStringInputOperator
    KafkaSinglePortInputOperator node = dag.addOperator("Kafka input", KafkaSinglePortInputOperator.class);
    node.setInitialPartitionCount(1);
    // set topic
    node.setTopics(TEST_TOPIC);
    node.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());
    node.setClusters(getClusterConfig());
    node.setStrategy(partition);

    // Create Test tuple collector
    CollectorModule collector = dag.addOperator("TestMessageCollector", new CollectorModule());

    // Connect ports
    dag.addStream("Kafka message", node.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    if (hasFailure) {
      setupHasFailureTest(node, dag);
    }
    lc.runAsync();

    // Wait 30s for consumer finish consuming all the messages
    boolean notTimeout = latch.await(40000, TimeUnit.MILLISECONDS);
    Assert.assertTrue("TIMEOUT: 40s Collected " + tupleCollection, notTimeout);

    // Check results
    Assert.assertEquals("Tuple count", totalCount, tupleCollection.size());
    logger.debug(String.format("Number of emitted tuples: %d", tupleCollection.size()));

    t.join();
    p.close();
    lc.shutdown();
  }

  private void setupHasFailureTest(KafkaSinglePortInputOperator operator, DAG dag)
  {
    operator.setHoldingBufferSize(5000);
    dag.setAttribute(Context.DAGContext.CHECKPOINT_WINDOW_COUNT, 1);
    //dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new FSStorageAgent("target/ck", new Configuration()));
    operator.setMaxTuplesPerWindow(500);
  }

  private String getClusterConfig() {
    String l = "localhost:";
    return l + TEST_KAFKA_BROKER_PORT[0][0] +
      (hasMultiPartition ? "," + l + TEST_KAFKA_BROKER_PORT[0][1] : "") +
      (hasMultiCluster ? ";" + l + TEST_KAFKA_BROKER_PORT[1][0] : "") +
      (hasMultiCluster && hasMultiPartition ? "," + l  + TEST_KAFKA_BROKER_PORT[1][1] : "");
  }


}
