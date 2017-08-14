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
import java.net.InetSocketAddress;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

/**
 * This is a base class setup/clean Kafka testing environment for all the input/output test If it's a multipartition
 * test, this class creates 2 kafka partitions
 */
public class KafkaOperatorTestBase
{

  public static final String END_TUPLE = "END_TUPLE";
  public static final int[] TEST_ZOOKEEPER_PORT = new int[] {2182, 2183};
  public static final int[][] TEST_KAFKA_BROKER_PORT = new int[][] {new int[] {9092, 9093}, new int[] {9094, 9095}};
  public static final String TEST_TOPIC = "test_topic";

  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaOperatorTestBase.class);
  // since Kafka 0.8 use KafkaServerStatble instead of KafkaServer

  // multiple brokers in multiple cluster
  private final KafkaServerStartable[][] broker = new KafkaServerStartable[2][2];

  // multiple cluster
  private final ServerCnxnFactory[] zkFactory = new ServerCnxnFactory[2];

  public String baseDir = "target";

  private final String zkBaseDir = "zookeeper-server-data";
  private final String kafkaBaseDir = "kafka-server-data";
  private final String[] zkdir = new String[] {"zookeeper-server-data/1", "zookeeper-server-data/2"};
  private final String[][] kafkadir = new String[][] {new String[] {"kafka-server-data/1/1", "kafka-server-data/1/2"}, new String[] {"kafka-server-data/2/1", "kafka-server-data/2/2"}};
  protected boolean hasMultiPartition = false;
  protected boolean hasMultiCluster = false;

  public void startZookeeper(final int clusterId)
  {

    try {
      //before start, clean the zookeeper files if it exists
      FileUtils.deleteQuietly(new File(baseDir, zkBaseDir));
      int clientPort = TEST_ZOOKEEPER_PORT[clusterId];
      int numConnections = 10;
      int tickTime = 2000;
      File dir = new File(baseDir, zkdir[clusterId]);

      TestZookeeperServer kserver = new TestZookeeperServer(dir, dir, tickTime);
      zkFactory[clusterId] = new NIOServerCnxnFactory();
      zkFactory[clusterId].configure(new InetSocketAddress(clientPort), numConnections);

      zkFactory[clusterId].startup(kserver); // start the zookeeper server.
      Thread.sleep(2000);
      kserver.startup();
    } catch (Exception ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  public void stopZookeeper()
  {
    for (ServerCnxnFactory zkf : zkFactory) {
      if (zkf != null) {
        zkf.shutdown();
      }
    }
  }

  public void startKafkaServer(int clusterid, int brokerid, int defaultPartitions)
  {
    // before start, clean the kafka dir if it exists
    FileUtils.deleteQuietly(new File(baseDir, kafkaBaseDir));

    Properties props = new Properties();
    props.setProperty("broker.id", "" + brokerid);
    props.setProperty("log.dirs", new File(baseDir, kafkadir[clusterid][brokerid]).toString());
    props.setProperty("zookeeper.connect", "localhost:" + TEST_ZOOKEEPER_PORT[clusterid]);
    props.setProperty("port", "" + TEST_KAFKA_BROKER_PORT[clusterid][brokerid]);
    props.setProperty("default.replication.factor", "1");
    // set this to 50000 to boost the performance so most test data are in memory before flush to disk
    props.setProperty("log.flush.interval.messages", "50000");
    if (hasMultiPartition) {
      props.setProperty("num.partitions", "2");
    } else {
      props.setProperty("num.partitions", "1");
    }

    broker[clusterid][brokerid] = new KafkaServerStartable(new KafkaConfig(props));
    broker[clusterid][brokerid].startup();

  }

  public void startKafkaServer()
  {
    boolean[][] startable = new boolean[][] {new boolean[] {true, hasMultiPartition }, new boolean[] {hasMultiCluster, hasMultiCluster && hasMultiPartition}};
    for (int i = 0; i < startable.length; i++) {
      for (int j = 0; j < startable[i].length; j++) {
        if (startable[i][j]) {
          startKafkaServer(i, j, hasMultiPartition ? 2 : 1);
        }
      }
    }

    // startup is asynch operation. wait 2 sec for server to startup
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  public void stopKafkaServer()
  {
    for (int i = 0; i < broker.length; i++) {
      for (int j = 0; j < broker[i].length; j++) {
        if (broker[i][j] != null) {
          broker[i][j].shutdown();
          broker[i][j].awaitShutdown();
        }
      }
    }
  }

  @Before
  public void beforeTest()
  {
    try {
      startZookeeper();
      startKafkaServer();
      createTopic(0, TEST_TOPIC);
      if (hasMultiCluster) {
        createTopic(1, TEST_TOPIC);
      }
    } catch (java.nio.channels.CancelledKeyException ex) {
      logger.debug("LSHIL {}", ex.getLocalizedMessage());
    }
  }

  public void startZookeeper()
  {
    startZookeeper(0);
    if (hasMultiCluster) {
      startZookeeper(1);
    }
  }

  public void createTopic(int clusterid, String topicName)
  {
    String[] args = new String[9];
    args[0] = "--zookeeper";
    args[1] = "localhost:" + TEST_ZOOKEEPER_PORT[clusterid];
    args[2] = "--replication-factor";
    args[3] = "1";
    args[4] = "--partitions";
    if (hasMultiPartition) {
      args[5] = "2";
    } else {
      args[5] = "1";
    }
    args[6] = "--topic";
    args[7] = topicName;
    args[8] = "--create";

    TopicCommand.main(args);
    // Right now, there is no programmatic synchronized way to create the topic. have to wait 2 sec to make sure the
    // topic is created
    // So the tests will not hit any bizarre failure
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @After
  public void afterTest()
  {
    try {
      stopKafkaServer();
      stopZookeeper();
    } catch (Exception ex) {
      logger.debug("LSHIL {}", ex.getLocalizedMessage());
    }
  }

  public void setHasMultiPartition(boolean hasMultiPartition)
  {
    this.hasMultiPartition = hasMultiPartition;
  }

  public void setHasMultiCluster(boolean hasMultiCluster)
  {
    this.hasMultiCluster = hasMultiCluster;
  }

  public static class TestZookeeperServer extends ZooKeeperServer
  {

    public TestZookeeperServer()
    {
      super();
      // TODO Auto-generated constructor stub
    }

    public TestZookeeperServer(File snapDir, File logDir, int tickTime) throws IOException
    {
      super(snapDir, logDir, tickTime);
      // TODO Auto-generated constructor stub
    }

    public TestZookeeperServer(FileTxnSnapLog txnLogFactory, DataTreeBuilder treeBuilder) throws IOException
    {
      super(txnLogFactory, treeBuilder);
      // TODO Auto-generated constructor stub
    }

    public TestZookeeperServer(FileTxnSnapLog txnLogFactory, int tickTime, DataTreeBuilder treeBuilder) throws IOException
    {
      super(txnLogFactory, tickTime, treeBuilder);
      // TODO Auto-generated constructor stub
    }

    public TestZookeeperServer(FileTxnSnapLog txnLogFactory, int tickTime, int minSessionTimeout, int maxSessionTimeout, DataTreeBuilder treeBuilder, ZKDatabase zkDb)
    {
      super(txnLogFactory, tickTime, minSessionTimeout, maxSessionTimeout, treeBuilder, zkDb);
      // TODO Auto-generated constructor stub
    }

    @Override
    protected void registerJMX()
    {
    }

    @Override
    protected void unregisterJMX()
    {
    }

  }
}
