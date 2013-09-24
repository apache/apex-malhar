/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
 * limitations under the License.
 */
package com.datatorrent.contrib.kafka;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Utils;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.slf4j.LoggerFactory;

/**
 * This is a base class setup/clean Kafka testing environment for all the input/output test 
 */
public class KafkaOperatorTestBase
{
  
  public static String END_TUPLE = "END_TUPLE";
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaOperatorTestBase.class);
  private KafkaServer kserver;
  private NIOServerCnxnFactory standaloneServerFactory;
  private final String zklogdir = "/tmp/zookeeper-server-data";
  private final String kafkalogdir = "/tmp/kafka-server-data";
  private boolean useZookeeper = true; // standard consumer use zookeeper, whereas simpleConsumer don't// standard consumer use zookeeper, whereas simpleConsumer don't


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
      standaloneServerFactory = new NIOServerCnxnFactory();
      standaloneServerFactory.configure(new InetSocketAddress(clientPort), numConnections);
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
      stopKafkaServer();
      stopZookeeper();
    }
    catch (java.nio.channels.CancelledKeyException ex) {
      logger.debug("LSHIL {}", ex.getLocalizedMessage());
    }
  }
  
  public boolean isUseZookeeper()
  {
    return useZookeeper;
  }
  
  public void setUseZookeeper(boolean useZookeeper)
  {
    this.useZookeeper = useZookeeper;
  }

}