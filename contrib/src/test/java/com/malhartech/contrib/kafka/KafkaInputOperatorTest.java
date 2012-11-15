/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.kafka;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import com.malhartech.stram.StramLocalCluster;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import junit.framework.Assert;
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

  public void startZookeeper()
  {
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
    standaloneServerFactory.shutdown();
    Utils.rm(zklogdir);
  }

  public void startKafkaServer()
  {
    Properties props = new Properties();
    props.setProperty("brokerid", "1");
    props.setProperty("log.dir", kafkalogdir);
    props.setProperty("enable.zookeeper", "true");
    props.setProperty("topic", "topic1");
    props.setProperty("log.flush.interval", "10"); // Controls the number of messages accumulated in each topic (partition) before the data is flushed to disk and made available to consumers.
    //   props.setProperty("log.default.flush.scheduler.interval.ms", "100");  // optional if we have the flush.interval
    props.setProperty("zk.connect", "localhost:2182");

    kserver = new KafkaServer(new KafkaConfig(props));
    kserver.startup();
  }

  public void stopKafkaServer()
  {
    kserver.shutdown();
    Utils.rm(kafkalogdir);
  }

  @Before
  public void beforeTest()
  {
    startZookeeper();
    startKafkaServer();
  }

  @After
  public void afterTest()
  {
    collections.clear();
    stopKafkaServer();
    stopZookeeper();
  }

  @Test
  public void testKafkaProducerConsumer() throws InterruptedException
  {
    // Start producer
    KafkaProducer p = new KafkaProducer("topic1");
    new Thread(p).start();
    Thread.sleep(1000);  // wait to flush message to disk and make available for consumer
    // p.close();

    // Start consumer
    KafkaConsumer c = new KafkaConsumer("topic1");
    //KafkaSimpleConsumer c = new KafkaSimpleConsumer();
    new Thread(c).start();
    Thread.sleep(1000); // make sure to consume all available message
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
      }
      catch (Exception e) {
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
      super(module);
      this.id = id;
    }

    @Override
    public void process(T tuple)
    {
      list.add(tuple);
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
  @Test
  public void testKafkaInputOperator() throws Exception
  {
    // Start producer
    KafkaProducer p = new KafkaProducer("topic1");
    new Thread(p).start();
    Thread.sleep(1000);  // wait to flush message to disk and make available for consumer
    p.close();

    // Create DAG for testing.
    DAG dag = new DAG();
    // Create KafkaStringSinglePortInputOperator
    KafkaStringSinglePortInputOperator node = dag.addOperator("Kafka message consumer", KafkaStringSinglePortInputOperator.class);

    // Create Test tuple collector
    CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("Kafka message", node.outputPort, collector.inputPort).setInline(true);

    // Create local cluster
    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    // Run local cluster
    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException ex) {
        }

        lc.shutdown();
      }
    }.start();
    lc.run();

    // Check results
    Assert.assertEquals("Collections size", 1, collections.size());
    Assert.assertEquals("Tuple count", 20, collections.get(collector.inputPort.id).size());
  }
}
