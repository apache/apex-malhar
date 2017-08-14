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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ShardManagerTest extends KinesisOperatorTestBase
{

  public ShardManagerTest()
  {
    hasMultiPartition = true;
  }

  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KinesisPartitionableInputOperatorTest.class);
  static List<String> collectedTuples = new LinkedList<String>();
  static final int totalCount = 100;
  static CountDownLatch latch;
  static final String OFFSET_FILE = ".offset";

  public static class TestShardManager extends ShardManager
  {
    private String filename = null;

    private transient FileSystem fs = FileSystem.get(new Configuration());

    private transient FileContext fc = FileContext.getFileContext(fs.getUri());

    public TestShardManager() throws IOException
    {

    }

    @Override
    public void updatePositions(Map<String, String> currentShardPos)
    {

      shardPos.putAll(currentShardPos);

      try {
        Path tmpFile = new Path(filename + ".tmp");
        Path dataFile = new Path(filename);
        FSDataOutputStream out = fs.create(tmpFile, true);
        for (Entry<String, String> e : shardPos.entrySet()) {
          out.writeBytes(e.getKey() + ", " + e.getValue() + "\n");
        }
        out.close();
        fc.rename(tmpFile, dataFile, Rename.OVERWRITE);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      countdownLatch();
    }

    private void countdownLatch()
    {
      if (latch.getCount() == 1) {
        // when latch is 1, it means consumer has consumed all the records
        int count = 0;
        for (Entry<String, String> entry : shardPos.entrySet()) {
          count++;
        }
        if (count == totalCount + 2) {
          // wait until all offsets add up to totalCount + 2 control END_TUPLE
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
   * Test Operator to collect tuples from KinesisSingleInputStringOperator.
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
      if (tuple.equals(KinesisOperatorTestBase.END_TUPLE)) {
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
   * Test ShardManager update positions in Consumer
   *
   * [Generate send 100 Data Recotds to Kinesis] ==> [wait until the offsets has been updated to 102 or timeout after 30s which means offset has not been updated]
   *
   *
   * @throws Exception
   */
  @Test
  public void testConsumerUpdateShardPos() throws Exception
  {
    // Create template simple consumer
    try {
      KinesisConsumer consumer = new KinesisConsumer();
      testPartitionableInputOperator(consumer);
    } finally {
      // clean test offset file
      cleanFile();
    }
  }

  private void cleanFile()
  {
    try {
      FileSystem.get(new Configuration()).delete(new Path(streamName + OFFSET_FILE), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void testPartitionableInputOperator(KinesisConsumer consumer) throws Exception
  {
    // Set to 3 because we want to make sure all the tuples from both 2 partitions are received and offsets has been updated to 102
    latch = new CountDownLatch(3);

    // Start producer
    KinesisTestProducer p = new KinesisTestProducer(streamName, true);
    p.setSendCount(totalCount);
    // wait the producer send all records
    p.run();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    // Create KinesisSinglePortStringInputOperator
    KinesisStringInputOperator node = dag.addOperator("Kinesis consumer", KinesisStringInputOperator.class);
    node.setAccessKey(credentials.getCredentials().getAWSSecretKey());
    node.setSecretKey(credentials.getCredentials().getAWSAccessKeyId());
    node.setStreamName(streamName);
    TestShardManager tfm = new TestShardManager();

    tfm.setFilename(streamName + OFFSET_FILE);

    node.setShardManager(tfm);

    node.setStrategy(AbstractKinesisInputOperator.PartitionStrategy.MANY_TO_ONE.toString());
    node.setRepartitionInterval(-1);

    //set topic
    consumer.setStreamName(streamName);
    //set the brokerlist used to initialize the partition
    consumer.setInitialOffset("earliest");

    node.setConsumer(consumer);

    // Create Test tuple collector
    CollectorModule collector = dag.addOperator("RecordCollector", new CollectorModule());

    // Connect ports
    dag.addStream("Kinesis Records", node.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);

    lc.runAsync();

    // Wait 15s for consumer finish consuming all the records
    latch.await(15000, TimeUnit.MILLISECONDS);

    // Check results
    assertEquals("Tuple count", totalCount, collectedTuples.size());
    logger.debug(String.format("Number of emitted tuples: %d -> %d", collectedTuples.size(), totalCount));

    lc.shutdown();
  }


  @Test
  public void testShardManager() throws Exception
  {
    // Set to 3 because we want to make sure all the tuples from both 2 partitions are received and offsets has been updated to 102
    latch = new CountDownLatch(3);

    // Start producer
    KinesisTestProducer p = new KinesisTestProducer(streamName, true);
    p.setSendCount(totalCount);
    // wait the producer send all records
    p.run();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    KinesisUtil.getInstance().setClient(client);
    // Create KinesisSinglePortStringInputOperator
    KinesisStringInputOperator node = dag.addOperator("Kinesis consumer", KinesisStringInputOperator.class);
    node.setAccessKey(credentials.getCredentials().getAWSSecretKey());
    node.setSecretKey(credentials.getCredentials().getAWSAccessKeyId());
    node.setStreamName(streamName);
    ShardManager tfm = new ShardManager();

    node.setShardManager(tfm);

    node.setStrategy(AbstractKinesisInputOperator.PartitionStrategy.MANY_TO_ONE.toString());
    node.setRepartitionInterval(-1);
    KinesisConsumer consumer = new KinesisConsumer();
    //set topic
    consumer.setStreamName(streamName);
    //set the brokerlist used to initialize the partition
    consumer.setInitialOffset("earliest");

    node.setConsumer(consumer);

    // Create Test tuple collector
    CollectorModule collector = dag.addOperator("RecordCollector", new CollectorModule());

    // Connect ports
    dag.addStream("Kinesis Records", node.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);

    lc.runAsync();

    // Wait 15s for consumer finish consuming all the records
    latch.await(10000, TimeUnit.MILLISECONDS);

    assertEquals("ShardPos Size", 2, node.getShardManager().loadInitialShardPositions().size());
    Iterator ite = node.getShardManager().loadInitialShardPositions().entrySet().iterator();
    Entry e = (Entry)ite.next();
    assertNotEquals("Record Seq No in Shard Id 1", "", e.getValue());
    e = (Entry)ite.next();
    assertNotEquals("Record Seq No in Shard Id 2", "", e.getValue());
    // Check results
    assertEquals("Tuple count", totalCount, collectedTuples.size());
    logger.debug(String.format("Number of emitted tuples: %d -> %d", collectedTuples.size(), totalCount));

    lc.shutdown();
  }
}
