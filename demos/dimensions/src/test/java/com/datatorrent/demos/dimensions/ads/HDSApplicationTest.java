/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.api.LocalMode;
import com.datatorrent.contrib.kafka.KafkaOperatorTestBase;
import com.datatorrent.contrib.kafka.KafkaTestConsumer;
import com.datatorrent.contrib.kafka.KafkaTestProducer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class HDSApplicationTest
{
  private static final Logger LOG = LoggerFactory.getLogger(HDSApplicationTest.class);
  private final KafkaOperatorTestBase kafkaLauncher = new KafkaOperatorTestBase();

  private static final String kafkaQueryTopic = "HDSApplicationQuery";
  private static final String kafkaQueryResultTopic = "HDSApplicationQueryResult";


  @Before
  public void beforeTest() throws Exception {
    kafkaLauncher.baseDir = "target/" + this.getClass().getName();
    FileUtils.deleteDirectory(new File(kafkaLauncher.baseDir));
    kafkaLauncher.startZookeeper();
    kafkaLauncher.startKafkaServer();
    kafkaLauncher.createTopic(kafkaQueryTopic);
    kafkaLauncher.createTopic(kafkaQueryResultTopic);
  }

  @After
  public void afterTest() {
    kafkaLauncher.stopKafkaServer();
    kafkaLauncher.stopZookeeper();
  }


  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.DimensionsComputation.attr.APPLICATION_WINDOW_COUNT", "1");

    conf.set("dt.operator.QueryResult.prop.configProperties(metadata.broker.list)", "localhost:9092");

    conf.set("dt.operator.Store.fileStore.basePath", "target/HDSApplicationTestStore");

    conf.set("dt.operator.Query.brokerSet", "localhost:9092");
    conf.set("dt.operator.Query.topic", kafkaQueryTopic);
    conf.set("dt.operator.QueryResult.topic", kafkaQueryResultTopic);

    conf.set("dt.operator.DimensionsComputation.attr.APPLICATION_WINDOW_COUNT", "2");
    conf.set("dt.operator.InputGenerator.numPublishers", "2");
    conf.set("dt.loggers.level", "server.*:INFO");

    lma.prepareDAG(new ApplicationWithHDS(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    //Write messages to kafkaQueryTopic
    KafkaTestProducer kafkaQuery = new KafkaTestProducer(kafkaQueryTopic);
    String testQuery="{\n" +
            " \"id\": \"query1\",\n" +
            " \"dimensionSelector\": \"time=MINUTES:publisherId\",\n" +
            " \"keys\": {\n" +
            "  \"publisherId\": 1\n" +
            " }\n" +
            "}";

    List<String> testQueryMessages = new ArrayList<String>();
    testQueryMessages.add(testQuery);
    kafkaQuery.setMessages(testQueryMessages);
    kafkaQuery.run();


    // Setup a message listener to receive the query results
    CountDownLatch latch = new CountDownLatch(100);
    KafkaTestConsumer queryResultsListener = new KafkaTestConsumer(kafkaQueryResultTopic);
    queryResultsListener.setLatch(latch);
    new Thread(queryResultsListener).start();

    // Wait to receive messages
    latch.await(15, TimeUnit.SECONDS);
    lc.shutdown();

    // Evaluate results
    String lastMessage;
    LOG.info("Sent " + kafkaQuery.getSendCount() + " messages to " + kafkaQueryTopic);
    LOG.info("Received " + queryResultsListener.holdingBuffer.size() + " messages from Kafka on " + kafkaQueryResultTopic + " topic");
    Assert.assertTrue("Minimum messages received from Kafka " + queryResultsListener.holdingBuffer, queryResultsListener.holdingBuffer.size() >= 1);

    while (!queryResultsListener.holdingBuffer.isEmpty()) {
      lastMessage = queryResultsListener.getMessage(queryResultsListener.holdingBuffer.poll());
      Assert.assertNotNull("Did not receive message from Kafka", lastMessage);
      LOG.info("received:\n{}", lastMessage);
    }
  }
}