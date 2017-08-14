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
package org.apache.apex.malhar.contrib.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

/**
 * A test to verify the input operator will be automatically partitioned per kafka partition This test is launching its
 * own Kafka cluster.
 */
@RunWith(Parameterized.class)
public class KafkaPartitionableInputOperatorTest extends KafkaOperatorTestBase
{

  private int totalBrokers = 0;

  @Parameterized.Parameters(name = "multi-cluster: {0}, multi-partition: {1}")
  public static Collection<Boolean[]> testScenario()
  {
    return Arrays.asList(new Boolean[][] {{true, false}, // multi cluster with single partition
        {true, true}, // multi cluster with multi partitions
        {false, true}, // single cluster with multi partitions
        {false, false} // single cluster with single partition
        });
  }

  public KafkaPartitionableInputOperatorTest(boolean hasMultiCluster, boolean hasMultiPartition)
  {
    // This class want to initialize several kafka brokers for multiple partitions
    this.hasMultiCluster = hasMultiCluster;
    this.hasMultiPartition = hasMultiPartition;
    int cluster = 1 + (hasMultiCluster ? 1 : 0);
    totalBrokers = (1 + (hasMultiPartition ? 1 : 0)) * cluster;

  }

  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaPartitionableInputOperatorTest.class);
  static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();
  static AtomicInteger tupleCount = new AtomicInteger();
  static CountDownLatch latch;

  /**
   * Test Operator to collect tuples from KafkaSingleInputStringOperator.
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
      if (tuple.equals(KafkaOperatorTestBase.END_TUPLE)) {
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
   * Test AbstractKafkaSinglePortInputOperator (i.e. an input adapter for Kafka, aka consumer). This module receives
   * data from an outside test generator through Kafka message bus and feed that data into Malhar streaming platform.
   *
   * [Generate message and send that to Kafka message bus] ==> [Receive that message through Kafka input adapter(i.e.
   * consumer) and send using emitTuples() interface on output port during onMessage call]
   *
   *
   * @throws Exception
   */
  @Test
  public void testPartitionableSimpleConsumerInputOperator() throws Exception
  {
    // Create template simple consumer
    SimpleKafkaConsumer consumer = new SimpleKafkaConsumer();
    testPartitionableInputOperator(consumer);
  }

  @Test
  public void testPartitionableHighlevelConsumerInputOperator() throws Exception
  {
    // Create template high-level consumer
    Properties props = new Properties();
    props.put("group.id", "main_group");
    HighlevelKafkaConsumer consumer = new HighlevelKafkaConsumer(props);
    testPartitionableInputOperator(consumer);
  }

  public void testPartitionableInputOperator(KafkaConsumer consumer) throws Exception
  {

    // each broker should get a END_TUPLE message
    latch = new CountDownLatch(totalBrokers);

    int totalCount = 10000;

    // Start producer
    KafkaTestProducer p = new KafkaTestProducer(TEST_TOPIC, hasMultiPartition, hasMultiCluster);
    p.setSendCount(totalCount);
    new Thread(p).start();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    // Create KafkaSinglePortStringInputOperator
    KafkaSinglePortStringInputOperator node = dag.addOperator("Kafka message consumer", KafkaSinglePortStringInputOperator.class);
    node.setInitialPartitionCount(1);

    // set topic
    consumer.setTopic(TEST_TOPIC);
    consumer.setInitialOffset("earliest");

    node.setConsumer(consumer);

    String clusterString = "cluster1::localhost:" + TEST_ZOOKEEPER_PORT[0] + (hasMultiCluster ? ";cluster2::localhost:" + TEST_ZOOKEEPER_PORT[1] : "");
    node.setZookeeper(clusterString);

    // Create Test tuple collector
    CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("Kafka message", node.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.runAsync();

    // Wait 30s for consumer finish consuming all the messages
    Assert.assertTrue("TIMEOUT: 40s ", latch.await(40000, TimeUnit.MILLISECONDS));

    // Check results
    Assert.assertEquals("Collections size", 1, collections.size());
    Assert.assertEquals("Tuple count", totalCount, collections.get(collector.inputPort.id).size());
    logger.debug(String.format("Number of emitted tuples: %d", collections.get(collector.inputPort.id).size()));

    p.close();
    lc.shutdown();
    // kafka has a bug shutdown connector you have to make sure kafka client resource has been cleaned before clean the broker
    Thread.sleep(5000);
  }


}
