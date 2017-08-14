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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.partitioner.StatelessPartitionerTest;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.FSStorageAgent;
import com.datatorrent.stram.StramLocalCluster;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;


public class KafkaInputOperatorTest extends KafkaOperatorTestBase
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaInputOperatorTest.class);
  static AtomicInteger tupleCount = new AtomicInteger();
  static CountDownLatch latch;
  static boolean isSuicide = false;
  static int suicideTrigger = 3000;

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

    private int k = 0;

    public CollectorInputPort(String id, Operator module)
    {
      super();
    }

    @Override
    public void process(T tuple)
    {
      if (isSuicide && k++ == suicideTrigger) {
        //you can only kill yourself once
        isSuicide = false;
        throw  new RuntimeException();
      }
      if (tuple.equals(KafkaOperatorTestBase.END_TUPLE)) {
        if (latch != null) {
          latch.countDown();
        }
        return;
      }
      tupleCount.incrementAndGet();
    }
  }

  /**
   * Test AbstractKafkaSinglePortInputOperator (i.e. an input adapter for
   * Kafka, aka consumer). This module receives data from an outside test
   * generator through Kafka message bus and feed that data into Malhar
   * streaming platform.
   *
   * [Generate message and send that to Kafka message bus] ==> [Receive that
   * message through Kafka input adapter(i.e. consumer) and send using
   * emitTuples() interface on output port during onMessage call]
   *
   *
   * @throws Exception
   */
  public void testKafkaInputOperator(int sleepTime, final int totalCount, KafkaConsumer consumer, boolean isValid, boolean idempotent) throws Exception
  {
    // initial the latch for this test
    latch = new CountDownLatch(1);


 // Start producer
    KafkaTestProducer p = new KafkaTestProducer(TEST_TOPIC);
    p.setSendCount(totalCount);
    new Thread(p).start();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();



    // Create KafkaSinglePortStringInputOperator
    KafkaSinglePortStringInputOperator node = dag.addOperator("Kafka message consumer", KafkaSinglePortStringInputOperator.class);
    if (isSuicide) {
      // make some extreme assumptions to make it fail if checkpointing wrong offsets
      dag.setAttribute(Context.DAGContext.CHECKPOINT_WINDOW_COUNT, 1);
      dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new FSStorageAgent("target/ck", new Configuration()));
      node.setMaxTuplesPerWindow(500);
    }

    if (idempotent) {
      node.setWindowDataManager(new FSWindowDataManager());
    }
    consumer.setTopic(TEST_TOPIC);

    node.setConsumer(consumer);

    consumer.setCacheSize(5000);

    if (isValid) {
      node.setZookeeper("localhost:" + KafkaOperatorTestBase.TEST_ZOOKEEPER_PORT[0]);
    }

    // Create Test tuple collector
    CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("Kafka message", node.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.runAsync();

    // Wait 30s for consumer finish consuming all the messages
    Assert.assertTrue("TIMEOUT: 30s ", latch.await(300000, TimeUnit.MILLISECONDS));

    // Check results
    Assert.assertTrue("Expected count >= " + totalCount + "; Actual count " + tupleCount.intValue(),
        totalCount <= tupleCount.intValue());
    logger.debug(String.format("Number of emitted tuples: %d", tupleCount.intValue()));

    p.close();
    lc.shutdown();
  }

  @Test
  public void testKafkaInputOperator_Highlevel() throws Exception
  {
    int totalCount = 10000;
    Properties props = new Properties();
    props.put("group.id", "group1");
    // This damn property waste me 2 days! It's a 0.8 new property. "smallest" means
    // reset the consumer to the beginning of the message that is not consumed yet
    // otherwise it wont get any of those the produced before!
    KafkaConsumer k = new HighlevelKafkaConsumer(props);
    k.setInitialOffset("earliest");
    testKafkaInputOperator(1000, totalCount, k, true, false);
  }

  @Test
  public void testKafkaInputOperator_Simple() throws Exception
  {
    int totalCount = 10000;
    KafkaConsumer k = new SimpleKafkaConsumer();
    k.setInitialOffset("earliest");
    testKafkaInputOperator(1000, totalCount, k, true, false);
  }

  @Test
  public void testKafkaInputOperator_SimpleSuicide() throws Exception
  {
    int totalCount = 10000;
    KafkaConsumer k = new SimpleKafkaConsumer();
    k.setInitialOffset("earliest");
    isSuicide = true;
    testKafkaInputOperator(1000, totalCount, k, true, false);
  }

  @Test
  public void testKafkaInputOperator_Simple_Idempotent() throws Exception
  {
    int totalCount = 10000;
    KafkaConsumer k = new SimpleKafkaConsumer();
    k.setInitialOffset("earliest");
    testKafkaInputOperator(1000, totalCount, k, true, true);
  }

  @Test
  public void testKafkaInputOperator_Invalid() throws Exception
  {
    int totalCount = 10000;
    SimpleKafkaConsumer consumer = new SimpleKafkaConsumer();
    try {
      testKafkaInputOperator(1000, totalCount,consumer, false, false);
    } catch (Exception e) {
      // invalid host setup expect to fail here
      Assert.assertEquals("Error creating local cluster", e.getMessage());
    }
  }

  @Override
  @Before
  public void beforeTest()
  {
    tupleCount.set(0);
    File syncCheckPoint = new File("target", "ck");
    File localFiles = new File("target" + StramLocalCluster.class.getName());
    try {
      FileUtils.deleteQuietly(syncCheckPoint);
      FileUtils.deleteQuietly(localFiles);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      super.beforeTest();
    }
  }

  public static class TestMeta extends TestWatcher
  {
    String baseDir;
    String recoveryDir;
    KafkaSinglePortStringInputOperator operator;
    CollectorTestSink<Object> sink;
    Context.OperatorContext context;

    @Override
    protected void starting(Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      baseDir = "target/" + className + "/" + methodName;
      recoveryDir = "recovery";
      try {
        FileUtils.deleteDirectory(new File(baseDir, "recovery"));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testRecoveryAndIdempotency() throws Exception
  {
    int totalCount = 1500;

    // initial the latch for this test
    latch = new CountDownLatch(50);

    // Start producer
    KafkaTestProducer p = new KafkaTestProducer(TEST_TOPIC);
    p.setSendCount(totalCount);
    new Thread(p).start();


    KafkaSinglePortStringInputOperator operator = createAndDeployOperator(true);
    latch.await(4000, TimeUnit.MILLISECONDS);
    operator.beginWindow(1);
    operator.emitTuples();
    operator.endWindow();
    operator.beginWindow(2);
    operator.emitTuples();
    operator.endWindow();

    //failure and then re-deployment of operator
    testMeta.sink.collectedTuples.clear();
    operator.teardown();
    operator.deactivate();

    operator = createAndDeployOperator(true);
    Assert.assertEquals("largest recovery window", 2, operator.getWindowDataManager().getLargestCompletedWindow());

    operator.beginWindow(1);
    operator.emitTuples();
    operator.endWindow();
    operator.beginWindow(2);
    operator.emitTuples();
    operator.endWindow();
    latch.await(3000, TimeUnit.MILLISECONDS);
    // Emiting data after all recovery windows are replayed
    operator.beginWindow(3);
    operator.emitTuples();
    operator.endWindow();

    Assert.assertEquals("Total messages collected ", totalCount, testMeta.sink.collectedTuples.size());
    testMeta.sink.collectedTuples.clear();
    operator.teardown();
    operator.deactivate();
  }

  @Test
  public void testRecoveryAndExactlyOnce() throws Exception
  {
    int totalCount = 1500;

    // initial the latch for this test
    latch = new CountDownLatch(50);

    // Start producer
    KafkaTestProducer p = new KafkaTestProducer(TEST_TOPIC);
    p.setSendCount(totalCount);
    new Thread(p).start();

    KafkaSinglePortStringInputOperator operator = createAndDeployOperator(false);
    latch.await(4000, TimeUnit.MILLISECONDS);
    operator.beginWindow(1);
    operator.emitTuples();
    operator.endWindow();
    operator.beginWindow(2);
    operator.emitTuples();
    operator.endWindow();
    operator.checkpointed(2);
    operator.committed(2);
    Map<KafkaPartition, Long> offsetStats = operator.offsetStats;
    int collectedTuplesAfterCheckpoint = testMeta.sink.collectedTuples.size();
    //failure and then re-deployment of operator
    testMeta.sink.collectedTuples.clear();
    operator.teardown();
    operator.deactivate();
    operator = createOperator(false);
    operator.offsetStats = offsetStats;
    operator.setup(testMeta.context);
    operator.activate(testMeta.context);
    latch.await(4000, TimeUnit.MILLISECONDS);
    // Emiting data after all recovery windows are replayed
    operator.beginWindow(3);
    operator.emitTuples();
    operator.endWindow();
    operator.beginWindow(4);
    operator.emitTuples();
    operator.endWindow();
    latch.await(3000, TimeUnit.MILLISECONDS);

    Assert.assertEquals("Total messages collected ", totalCount - collectedTuplesAfterCheckpoint + 1, testMeta.sink.collectedTuples.size());
    testMeta.sink.collectedTuples.clear();
    operator.teardown();
    operator.deactivate();
  }

  private KafkaSinglePortStringInputOperator createOperator(boolean isIdempotency)
  {
    Attribute.AttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(Context.OperatorContext.SPIN_MILLIS, 500);
    attributeMap.put(Context.DAGContext.APPLICATION_PATH, testMeta.baseDir);


    testMeta.context = mockOperatorContext(1, attributeMap);
    testMeta.operator = new KafkaSinglePortStringInputOperator();

    KafkaConsumer consumer = new SimpleKafkaConsumer();
    consumer.setTopic(TEST_TOPIC);
    consumer.setInitialOffset("earliest");

    if (isIdempotency) {
      FSWindowDataManager storageManager = new FSWindowDataManager();
      storageManager.setStatePath(testMeta.recoveryDir);
      testMeta.operator.setWindowDataManager(storageManager);
    }

    testMeta.operator.setConsumer(consumer);
    testMeta.operator.setZookeeper("localhost:" + KafkaOperatorTestBase.TEST_ZOOKEEPER_PORT[0]);
    testMeta.operator.setMaxTuplesPerWindow(500);

    List<Partitioner.Partition<AbstractKafkaInputOperator<KafkaConsumer>>> partitions = new LinkedList<Partitioner.Partition<AbstractKafkaInputOperator<KafkaConsumer>>>();

    Collection<Partitioner.Partition<AbstractKafkaInputOperator<KafkaConsumer>>> newPartitions = testMeta.operator.definePartitions(partitions, new StatelessPartitionerTest.PartitioningContextImpl(null, 0));
    Assert.assertEquals(1, newPartitions.size());

    KafkaSinglePortStringInputOperator operator = (KafkaSinglePortStringInputOperator)newPartitions.iterator().next().getPartitionedInstance();

    testMeta.sink = new CollectorTestSink<Object>();
    testMeta.operator.outputPort.setSink(testMeta.sink);
    operator.outputPort.setSink(testMeta.sink);
    return operator;
  }

  private KafkaSinglePortStringInputOperator createAndDeployOperator(boolean isIdempotency)
  {
    KafkaSinglePortStringInputOperator operator = createOperator(isIdempotency);
    operator.setup(testMeta.context);
    operator.activate(testMeta.context);

    return operator;

  }

  @Test
  public void testMaxTotalSize() throws InterruptedException
  {
    int totalCount = 1500;
    int maxTotalSize = 500;

    // initial the latch for this test
    latch = new CountDownLatch(1);

    // Start producer
    KafkaTestProducer p = new KafkaTestProducer(TEST_TOPIC);
    p.setSendCount(totalCount);
    Thread t = new Thread(p);
    t.start();

    Attribute.AttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(Context.DAGContext.APPLICATION_PATH, testMeta.baseDir);

    OperatorContext context = mockOperatorContext(1, attributeMap);
    KafkaSinglePortStringInputOperator operator = new KafkaSinglePortStringInputOperator();

    KafkaConsumer consumer = new SimpleKafkaConsumer();
    consumer.setTopic(TEST_TOPIC);
    consumer.setInitialOffset("earliest");

    operator.setConsumer(consumer);
    operator.setZookeeper("localhost:" + KafkaOperatorTestBase.TEST_ZOOKEEPER_PORT[0]);
    operator.setMaxTotalMsgSizePerWindow(maxTotalSize);

    List<Partitioner.Partition<AbstractKafkaInputOperator<KafkaConsumer>>> partitions = new LinkedList<Partitioner.Partition<AbstractKafkaInputOperator<KafkaConsumer>>>();

    Collection<Partitioner.Partition<AbstractKafkaInputOperator<KafkaConsumer>>> newPartitions = operator.definePartitions(partitions, new StatelessPartitionerTest.PartitioningContextImpl(null, 0));
    Assert.assertEquals(1, newPartitions.size());

    operator = (KafkaSinglePortStringInputOperator)newPartitions.iterator().next().getPartitionedInstance();

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    operator.outputPort.setSink(sink);
    operator.setup(context);
    operator.activate(context);
    latch.await(4000, TimeUnit.MILLISECONDS);
    operator.beginWindow(1);
    operator.emitTuples();
    operator.endWindow();

    t.join();

    operator.deactivate();
    operator.teardown();
    int size = 0;
    for (Object o : sink.collectedTuples) {
      size += ((String)o).getBytes().length;
    }
    Assert.assertTrue("Total emitted size comparison", size < maxTotalSize);
  }

  @Test
  public void testZookeeper() throws Exception
  {
    // initial the latch for this test
    latch = new CountDownLatch(50);

    testMeta.operator = new KafkaSinglePortStringInputOperator();

    KafkaConsumer consumer = new SimpleKafkaConsumer();
    consumer.setTopic(TEST_TOPIC);

    testMeta.operator.setConsumer(consumer);
    testMeta.operator.setZookeeper("cluster1::node0,node1,node2:2181,node3:2182/chroot/dir;cluster2::node4:2181");
    latch.await(500, TimeUnit.MILLISECONDS);

    Assert.assertEquals("Total size of clusters ", 2, testMeta.operator.getConsumer().zookeeperMap.size());
    Assert.assertEquals("Connection url for cluster1 ", "node0,node1,node2:2181,node3:2182/chroot/dir", testMeta.operator.getConsumer().zookeeperMap.get("cluster1").iterator().next());
    Assert.assertEquals("Connection url for cluster 2 ", "node4:2181", testMeta.operator.getConsumer().zookeeperMap.get("cluster2").iterator().next());
  }

}
