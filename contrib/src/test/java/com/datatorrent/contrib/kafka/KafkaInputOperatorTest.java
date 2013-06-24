/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.contrib.kafka;

import com.datatorrent.contrib.kafka.KafkaSinglePortInputOperator;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.WaitCondition;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.Assert;
import kafka.consumer.ConsumerConfig;
import kafka.message.Message;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Utils;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class KafkaInputOperatorTest
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaInputOperatorTest.class);
  private static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();
  private KafkaServer kserver;
  private NIOServerCnxn.Factory standaloneServerFactory;
  private final String zklogdir = "/tmp/zookeeper-server-data";
  private final String kafkalogdir = "/tmp/kafka-server-data";
  private final boolean useZookeeper = true;  // standard consumer use zookeeper, whereas simpleConsumer don't
  static AtomicInteger tupleCount = new AtomicInteger();

  public void startZookeeper()
  {
    if (!useZookeeper) { // Do not use zookeeper for simpleconsumer
      return;
    }

    try {
      int clientPort = 2182;
      int numConnections = 5000;
      int tickTime = 2000;
      File dir = new File(zklogdir);

      ZooKeeperServer zserver = new ZooKeeperServer(dir, dir, tickTime);
      standaloneServerFactory = new NIOServerCnxn.Factory(new InetSocketAddress(clientPort), numConnections);
      standaloneServerFactory.startup(zserver); // start the zookeeper server.
    }
    catch (InterruptedException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
    catch (IOException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  public void stopZookeeper()
  {
    if (!useZookeeper) {
      return;
    }

    standaloneServerFactory.shutdown();
    Utils.rm(zklogdir);
  }

  public void startKafkaServer()
  {
    Properties props = new Properties();
    if (useZookeeper) {
      props.setProperty("enable.zookeeper", "true");
      props.setProperty("zk.connect", "localhost:2182");
      props.setProperty("topic", "topic1");
      props.setProperty("log.flush.interval", "10"); // Controls the number of messages accumulated in each topic (partition) before the data is flushed to disk and made available to consumers.
      //   props.setProperty("log.default.flush.scheduler.interval.ms", "100");  // optional if we have the flush.interval
    }
    else {
      props.setProperty("enable.zookeeper", "false");
      props.setProperty("hostname", "localhost");
      props.setProperty("port", "2182");
    }
    props.setProperty("brokerid", "1");
    props.setProperty("log.dir", kafkalogdir);

    kserver = new KafkaServer(new KafkaConfig(props));
    kserver.startup();
  }

  public void stopKafkaServer()
  {
    kserver.shutdown();
    kserver.awaitShutdown();
    Utils.rm(kafkalogdir);
  }

  @Before
  public void beforeTest()
  {
    try {
      startZookeeper();
      startKafkaServer();
    }
    catch (java.nio.channels.CancelledKeyException ex) {
      logger.debug("LSHIL {}", ex.getLocalizedMessage());
    }
  }

  @After
  public void afterTest()
  {
    try {
      collections.clear();
      stopKafkaServer();
      stopZookeeper();
    }
    catch (java.nio.channels.CancelledKeyException ex) {
      logger.debug("LSHIL {}", ex.getLocalizedMessage());
    }
  }

  //@Test
  public void testKafkaProducerConsumer() throws InterruptedException
  {
    // Start producer
    KafkaProducer p = new KafkaProducer("topic1", false);
    new Thread(p).start();
    Thread.sleep(1000);  // wait to flush message to disk and make available for consumer
    p.close();

    // Start consumer
    KafkaConsumer c = new KafkaConsumer("topic1");
    new Thread(c).start();
    Thread.sleep(1000); // make sure to consume all available message
    c.setIsAlive(true);
    c.close();

    // Check send vs receive message
    Assert.assertEquals("Message count: ", p.getSendCount(), c.getReceiveCount());
  }

  //  @Test
  public void testKafkaProducerSimpleConsumer() throws InterruptedException
  {
    // Start producer
    KafkaProducer p = new KafkaProducer("topic1", true);
    new Thread(p).start();
    Thread.sleep(1000);  // wait to flush message to disk and make available for consumer
    p.close();

    // Start consume
    KafkaSimpleConsumer c = new KafkaSimpleConsumer();
    new Thread(c).start();
    Thread.sleep(10000); // make sure to consume all available message; need more time for simple consumer
    c.setIsAlive(true);
    c.close();

    // Check send vs receive message
    Assert.assertEquals("Message count: ", p.getSendCount(), c.getReceiveCount());
  }

  // ==================================
  /**
   * An example Concrete class of KafkaSinglePortInputOperator for testing.
   */
  public static class KafkaStringSinglePortInputOperator extends KafkaSinglePortInputOperator<String>
  {
    @Override
    public ConsumerConfig createKafkaConsumerConfig()
    {
      Properties props = new Properties();
      props.put("zk.connect", "localhost:2182");
      props.put("groupid", "group1");
      //props.put("zk.sessiontimeout.ms", "400");
      //props.put("zk.synctime.ms", "200");
      //props.put("autocommit.interval.ms", "1000");
      return new ConsumerConfig(props);
    }

    /**
     * Implement abstract method of AbstractActiveMQSinglePortInputOperator
     */
    @Override
    public String getTuple(Message message)
    {
      String data = "";
      try {
        ByteBuffer buffer = message.payload();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        data = new String(bytes);
        //logger.debug("Consuming {}", data);
      }
      catch (Exception ex) {
        return data;
      }
      return data;
    }
  } // End of KafkaStringSinglePortInputOperator

  /**
   * Test Operator to collect tuples from ActiveMQStringSinglePortInputOperator.
   *
   * @param <T>
   */
  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("myInput", this);
  }

  /**
   * Test Operator to collect tuples from ActiveMQMultiPortInputOperator.
   *
   * @param <T1, T2>
   */
  public static class CollectorModule2<T1, T2> extends BaseOperator
  {
    public final transient CollectorInputPort<T1> inputPort1 = new CollectorInputPort<T1>("myInput1", this);
    public final transient CollectorInputPort<T2> inputPort2 = new CollectorInputPort<T2>("myInput2", this);
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
   * Test KafkaSinglePortInputOperator (i.e. an input adapter for ActiveMQ, aka consumer).
   * This module receives data from an outside test generator through Kafka message bus and
   * feed that data into Malhar streaming platform.
   *
   * [Generate message and send that to Kafka message bus] ==>
   * [Receive that message through Kafka input adapter(i.e. consumer) and send using emitTuples() interface on output port during onMessage call]
   *
   *
   * @throws Exception
   */
  public void testKafkaInputOperator(boolean isSimple, String consumerType, int sleepTime, final int totalCount) throws Exception
  {
    // Start producer
    KafkaProducer p = new KafkaProducer("topic1", isSimple);
    p.setSendCount(totalCount);
    new Thread(p).start();
    //Thread.sleep(sleepTime);  // wait to flush message to disk and make available for consumer
    //p.close();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    // Create KafkaStringSinglePortInputOperator
    KafkaStringSinglePortInputOperator node = dag.addOperator("Kafka message consumer", KafkaStringSinglePortInputOperator.class);
    if (consumerType.equals("standard")) {
      node.setConsumerType("standard");
    }
    else {
      node.setConsumerType("simple");
    }

    // Create Test tuple collector
    CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("Kafka message", node.outputPort, collector.inputPort).setInline(true);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.runAsync();
    WaitCondition c = new WaitCondition() {
      @Override
      public boolean isComplete() {
        return tupleCount.get() > totalCount;
      }
    };
    StramTestSupport.awaitCompletion(c, 26000);  // 10k tuples takes 13 sec => 770 tuple/sec

    lc.shutdown();

    // Check results
    Assert.assertEquals("Collections size", 1, collections.size());
    Assert.assertEquals("Tuple count", totalCount, collections.get(collector.inputPort.id).size());
    logger.debug(String.format("Number of emitted tuples: %d", collections.get(collector.inputPort.id).size()));

    p.close();
  }

  @Test
  public void testKafkaInputOperator_standard() throws Exception
  {
    int totalCount = 10000;
    testKafkaInputOperator(false, "standard", 1000, totalCount);
    //testKafkaInputOperator(true, "simple", 10000); // simpleConsumer
  }
}
