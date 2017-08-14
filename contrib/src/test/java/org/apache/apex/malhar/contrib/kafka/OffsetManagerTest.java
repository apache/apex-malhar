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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.kafka.AbstractKafkaInputOperator.PartitionStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OffsetManagerTest extends KafkaOperatorTestBase
{

  public OffsetManagerTest()
  {
    // This class want to initialize several kafka brokers for multiple partitions
    hasMultiPartition = true;
  }

  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaPartitionableInputOperatorTest.class);
  static List<String> collectedTuples = new LinkedList<String>();
  static final int totalCount = 100;
  static CountDownLatch latch;
  static final String OFFSET_FILE = ".offset";
  static long initialPos = 10L;
  static Path baseFolder = new Path("target");


  public static class TestOffsetManager implements OffsetManager
  {
    private final transient Map<KafkaPartition, Long> offsets = Collections.synchronizedMap(new HashMap<KafkaPartition, Long>());

    private String filename = null;

    private transient FileSystem fs = FileSystem.get(new Configuration());

    private transient FileContext fc = FileContext.getFileContext(fs.getUri());

    public TestOffsetManager() throws IOException
    {

    }

    @Override
    public Map<KafkaPartition, Long> loadInitialOffsets()
    {
      KafkaPartition kp0 = new KafkaPartition(TEST_TOPIC, 0);
      KafkaPartition kp1 = new KafkaPartition(TEST_TOPIC, 1);
      offsets.put(kp0, initialPos);
      offsets.put(kp1, initialPos);
      return offsets;
    }

    @Override
    public void updateOffsets(Map<KafkaPartition, Long> offsetsOfPartitions)
    {
      offsets.putAll(offsetsOfPartitions);
      try {
        Path tmpFile = new Path(baseFolder, filename + ".tmp");
        Path dataFile = new Path(baseFolder, filename);
        FSDataOutputStream out = fs.create(tmpFile, true);
        for (Entry<KafkaPartition, Long> e : offsets.entrySet()) {
          out.writeBytes(e.getKey() + ", " + e.getValue() + "\n");
        }
        out.close();
        fc.rename(tmpFile, dataFile, Rename.OVERWRITE);
      } catch (Exception e) {
        //
      }
      countdownLatch();
    }

    private void countdownLatch()
    {
      if (latch.getCount() == 1) {
        // when latch is 1, it means consumer has consumed all the messages
        int count = 0;
        for (long entry : offsets.values()) {
          count += entry;
        }
        if (count == totalCount) {
          // wait until all offsets add up to totalCount messages + 2 control END_TUPLE
          latch.countDown();
        }
      }
    }

    public void setFilename(String filename)
    {
      this.filename = filename;
    }

    public String getFilename()
    {
      return filename;
    }

  }

  /**
   * Test Operator to collect tuples from KafkaSingleInputStringOperator.
   *
   * @param
   */
  public static class CollectorModule extends BaseOperator
  {
    public final transient CollectorInputPort inputPort = new CollectorInputPort(this);
  }

  public static class CollectorInputPort extends DefaultInputPort<String>
  {

    public CollectorInputPort(Operator module)
    {
      super();
    }

    @Override
    public void process(String tuple)
    {
      if (tuple.equals(KafkaOperatorTestBase.END_TUPLE)) {
        if (latch != null) {
          latch.countDown();
        }
        return;
      }
      collectedTuples.add(tuple);
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        collectedTuples.clear();
      }
    }
  }

  /**
   * Test OffsetManager update offsets in Simple Consumer
   *
   * [Generate send 100 messages to Kafka] ==> [wait until the offsets has been updated to 102 or timeout after 30s which means offset has not been updated]
   *
   *
   * @throws Exception
   */
  @Test
  public void testSimpleConsumerUpdateOffsets() throws Exception
  {
    initialPos = 10L;
    // Create template simple consumer
    try {
      SimpleKafkaConsumer consumer = new SimpleKafkaConsumer();
      testPartitionableInputOperator(consumer, totalCount - (int)initialPos - (int)initialPos);
    } finally {
      // clean test offset file
      cleanFile();
    }
  }

  /**
   * Test OffsetManager update offsets in Simple Consumer
   *
   * [Generate send 100 messages to Kafka] ==> [wait until the offsets has been updated to 102 or timeout after 30s which means offset has not been updated]
   *
   * Initial offsets are invalid, reset to ealiest and get all messages
   *
   * @throws Exception
   */
  @Test
  public void testSimpleConsumerInvalidInitialOffsets() throws Exception
  {
    initialPos = 1000L;
    // Create template simple consumer
    try {
      SimpleKafkaConsumer consumer = new SimpleKafkaConsumer();
      testPartitionableInputOperator(consumer, totalCount);
    } finally {
      // clean test offset file
      cleanFile();
    }
  }

  private void cleanFile()
  {
    try {
      FileSystem.get(new Configuration()).delete(new Path(baseFolder, TEST_TOPIC + OFFSET_FILE), true);
    } catch (IOException e) {
      //
    }
  }

  public void testPartitionableInputOperator(KafkaConsumer consumer, int expectedCount) throws Exception
  {
    // Set to 3 because we want to make sure END_TUPLE from both 2 partitions are received and offsets has been updated to 102
    latch = new CountDownLatch(3);

    // Start producer
    KafkaTestProducer p = new KafkaTestProducer(TEST_TOPIC, true);
    p.setProducerType("sync");
    p.setSendCount(totalCount);
    // wait the producer send all messages
    p.run();
    p.close();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    // Create KafkaSinglePortStringInputOperator
    KafkaSinglePortStringInputOperator node = dag.addOperator("Kafka message consumer", KafkaSinglePortStringInputOperator.class);


    TestOffsetManager tfm = new TestOffsetManager();

    tfm.setFilename(TEST_TOPIC + OFFSET_FILE);

    node.setInitialPartitionCount(1);
    node.setOffsetManager(tfm);
    node.setStrategy(PartitionStrategy.ONE_TO_MANY.toString());
    node.setRepartitionInterval(-1);

    //set topic
    consumer.setTopic(TEST_TOPIC);
    //set the zookeeper list used to initialize the partition
    SetMultimap<String, String> zookeeper = HashMultimap.create();
    String zks = "localhost:" + KafkaOperatorTestBase.TEST_ZOOKEEPER_PORT[0];
    consumer.setZookeeper(zks);
    consumer.setInitialOffset("earliest");

    node.setConsumer(consumer);

    // Create Test tuple collector
    CollectorModule collector = dag.addOperator("TestMessageCollector", new CollectorModule());

    // Connect ports
    dag.addStream("Kafka message", node.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    dag.setAttribute(Context.DAGContext.CHECKPOINT_WINDOW_COUNT, 1);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);

    lc.runAsync();



    boolean isNotTimeout = latch.await(30000, TimeUnit.MILLISECONDS);
    // Wait 30s for consumer finish consuming all the messages and offsets has been updated to 100
    assertTrue("TIMEOUT: 30s, collected " + collectedTuples.size() + " tuples", isNotTimeout);

    // Check results
    assertEquals("Tuple count " + collectedTuples, expectedCount, collectedTuples.size());
    logger.debug(String.format("Number of emitted tuples: %d", collectedTuples.size()));

    p.close();
    lc.shutdown();
  }

}
