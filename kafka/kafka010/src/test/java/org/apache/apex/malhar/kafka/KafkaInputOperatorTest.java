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

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.FSWindowDataManager;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;

/**
 * A bunch of test to verify the input operator will be automatically partitioned
 * per kafka partition This test is launching its
 * own Kafka cluster.
 */
@Ignore // see https://issues.apache.org/jira/browse/APEXMALHAR-2162
@RunWith(Parameterized.class)
public class KafkaInputOperatorTest extends KafkaOperatorTestBase
{

  private int totalBrokers = 0;

  private String partition = null;

  private String testName = "";

  public static String APPLICATION_PATH = baseDir + File.separator + StramLocalCluster.class.getName() + File.separator;

  public class KafkaTestInfo extends TestWatcher
  {
    public org.junit.runner.Description desc;

    public String getDir()
    {
      String methodName = desc.getMethodName();
      String className = desc.getClassName();
      return "target/" + className + "/" + methodName + "/" + testName;
    }

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.desc = description;
    }
  }

  @Rule
  public final KafkaTestInfo testInfo = new KafkaTestInfo();

  @Parameterized.Parameters(name = "multi-cluster: {0}, multi-partition: {1}, partition: {2}")
  public static Collection<Object[]> testScenario()
  {
    return Arrays.asList(new Object[][]{
      {true, false, "one_to_one"},// multi cluster with single partition
      {true, false, "one_to_many"},
      {true, true, "one_to_one"},// multi cluster with multi partitions
      {true, true, "one_to_many"},
      {false, true, "one_to_one"}, // single cluster with multi partitions
      {false, true, "one_to_many"},
      {false, false, "one_to_one"}, // single cluster with single partitions
      {false, false, "one_to_many"}
    });
  }

  @Before
  public void before()
  {
    testName = TEST_TOPIC + testCounter++;
    logger.info("before() test case: {}", testName);
    tupleCollection.clear();
    //reset count for next new test case
    k = 0;

    createTopic(0, testName);
    if (hasMultiCluster) {
      createTopic(1, testName);
    }

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

  /**
   * whether countDown latch count all tuples or just END_TUPLE
   */
  private static final boolean countDownAll = false;
  private static final int scale = 2;
  private static final int totalCount = 10 * scale;
  private static final int failureTrigger = 3 * scale;
  private static final int tuplesPerWindow = 5 * scale;
  private static final int waitTime = 60000 + 300 * scale;

  //This latch was used to count the END_TUPLE, but the order of tuple can't be guaranteed,
  //so, count valid tuple instead.
  private static CountDownLatch latch;
  private static boolean hasFailure = false;
  private static int k = 0;
  private static Thread monitorThread;

  /**
   * Test Operator to collect tuples from KafkaSingleInputStringOperator.
   *
   * @param
   */
  public static class CollectorModule extends BaseOperator
  {
    public final transient DefaultInputPort<byte[]> inputPort = new DefaultInputPort<byte[]>()
    {
      @Override
      public void process(byte[] bt)
      {
        processTuple(bt);
      }
    };

    long currentWindowId;

    long operatorId;

    boolean isIdempotentTest = false;

    transient List<String> windowTupleCollector = Lists.newArrayList();
    private transient Map<String, List<String>> tupleCollectedInWindow = new HashMap<>();
    private int endTuples = 0;

    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      operatorId = context.getId();
    }

    @Override
    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
      currentWindowId = windowId;
      windowTupleCollector.clear();
      endTuples = 0;
    }

    public void processTuple(byte[] bt)
    {
      String tuple = new String(bt);
      if (hasFailure && k++ == failureTrigger) {
        //you can only kill yourself once
        hasFailure = false;
        throw new RuntimeException();
      }
      if (tuple.startsWith(KafkaOperatorTestBase.END_TUPLE)) {
        endTuples++;
      }

      windowTupleCollector.add(tuple);
    }

    @Override
    public void endWindow()
    {
      super.endWindow();
      if (isIdempotentTest) {
        String key = operatorId + "," + currentWindowId;
        List<String> msgsInWin = tupleCollectedInWindow.get(key);
        if (msgsInWin != null) {
          Assert.assertEquals(
              "replay messages should be exactly same as previous window", msgsInWin, windowTupleCollector);
        } else {
          List<String> newList = Lists.newArrayList();
          newList.addAll(windowTupleCollector);
          tupleCollectedInWindow.put(key, newList);
        }
      }

      //discard the tuples of this window if except happened
      int tupleSize = windowTupleCollector.size();
      tupleCollection.addAll(windowTupleCollector);

      int countDownTupleSize = countDownAll ? tupleSize : endTuples;

      if (latch != null) {
        Assert.assertTrue(
            "received END_TUPLES more than expected.", latch.getCount() >= countDownTupleSize);
        while (countDownTupleSize > 0) {
          latch.countDown();
          --countDownTupleSize;
        }
        if (latch.getCount() == 0) {
          /**
           * The time before countDown() and the shutdown() of the application
           * will cause fatal error:
           * "Catastrophic Error: Invalid State - the operator blocked forever!"
           * as the activeQueues could be cleared but alive haven't changed yet.
           * throw the ShutdownException to let the engine shutdown;
           */
          try {
            throw new ShutdownException();
            //lc.shutdown();
          } finally {
            /**
             * interrupt the engine thread, let it wake from sleep and handle
             * the shutdown at this time, all payload should be handled. so it
             * should be ok to interrupt
             */
            monitorThread.interrupt();
          }
        }
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
  public void testInputOperator() throws Exception
  {
    hasFailure = false;
    testInputOperator(false, false);
  }

  @Test
  public void testInputOperatorWithFailure() throws Exception
  {
    hasFailure = true;
    testInputOperator(true, false);
  }

  @Test
  public void testIdempotentInputOperatorWithFailure() throws Exception
  {
    hasFailure = true;
    testInputOperator(true, true);
  }

  public void testInputOperator(boolean hasFailure, boolean idempotent) throws Exception
  {
    // each broker should get a END_TUPLE message
    latch = new CountDownLatch(countDownAll ? totalCount + totalBrokers : totalBrokers);

    logger.info(
        "Test Case: name: {}; totalBrokers: {}; hasFailure: {}; hasMultiCluster: {};" +
        " hasMultiPartition: {}, partition: {}",
        testName, totalBrokers, hasFailure, hasMultiCluster, hasMultiPartition, partition);

    // Start producer
    KafkaTestProducer p = new KafkaTestProducer(testName, hasMultiPartition, hasMultiCluster);
    p.setSendCount(totalCount);
    Thread t = new Thread(p);
    t.start();

    int expectedReceiveCount = totalCount + totalBrokers;

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    // Create KafkaSinglePortStringInputOperator
    KafkaSinglePortInputOperator node = dag.addOperator(
        "Kafka input" + testName, KafkaSinglePortInputOperator.class);
    node.setInitialPartitionCount(1);
    // set topic
    node.setTopics(testName);
    node.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());
    node.setClusters(getClusterConfig());
    node.setStrategy(partition);
    if (idempotent) {
      node.setWindowDataManager(new FSWindowDataManager());
    }

    // Create Test tuple collector
    CollectorModule collector = dag.addOperator("TestMessageCollector", CollectorModule.class);
    collector.isIdempotentTest = idempotent;

    // Connect ports
    dag.addStream("Kafka message" + testName, node.outputPort, collector.inputPort)
        .setLocality(Locality.CONTAINER_LOCAL);

    if (hasFailure) {
      setupHasFailureTest(node, dag);
    }

    // Create local cluster
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    //let the Controller to run the inside another thread. It is almost same as call Controller.runAsync(),
    //but Controller.runAsync() don't expose the thread which run it,
    //so we don't know when the thread will be terminated.
    //create this thread and then call join() to make sure the Controller shutdown completely.
    monitorThread = new Thread((StramLocalCluster)lc, "master");
    monitorThread.start();

    boolean notTimeout = true;
    try {
      // Wait 60s for consumer finish consuming all the messages
      notTimeout = latch.await(waitTime, TimeUnit.MILLISECONDS);
      lc.shutdown();

      //wait until control thread finished.
      monitorThread.join();
    } catch (Exception e) {
      logger.warn(e.getMessage());
    }

    t.join();

    if (!notTimeout || expectedReceiveCount != tupleCollection.size()) {
      logger.info("Number of received/expected tuples: {}/{}, testName: {}, tuples: \n{}", tupleCollection.size(),
          expectedReceiveCount, testName, tupleCollection);
    }
    Assert.assertTrue("TIMEOUT. testName: " + this.testName + "; Collected data: "
        + tupleCollection, notTimeout);

    // Check results
    Assert.assertTrue("testName: " + testName + "; Collected tuple size: " + tupleCollection.size()
        + "; Expected tuple size: " + expectedReceiveCount + "; data: \n" + tupleCollection,
        expectedReceiveCount == tupleCollection.size());

    logger.info("End of test case: {}", testName);
  }

  private void setupHasFailureTest(KafkaSinglePortInputOperator operator, DAG dag)
  {
    operator.setHoldingBufferSize(5000);
    dag.setAttribute(Context.DAGContext.CHECKPOINT_WINDOW_COUNT, 1);
    //dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new FSStorageAgent(
    //  APPLICATION_PATH + "failureck", new Configuration()));
    operator.setMaxTuplesPerWindow(tuplesPerWindow);
  }

  private String getClusterConfig()
  {
    String l = "localhost:";
    return l + TEST_KAFKA_BROKER_PORT[0] +
      (hasMultiCluster ? ";" + l + TEST_KAFKA_BROKER_PORT[1] : "");
  }

}
