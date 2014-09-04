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
package com.datatorrent.demos.adsdimension;

import com.datatorrent.api.LocalMode;
import com.datatorrent.contrib.kafka.KafkaOperatorTestBase;
import com.datatorrent.contrib.kafka.KafkaTestConsumer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaApplicationTest
{
  private static final Logger LOG = LoggerFactory.getLogger(KafkaApplicationTest.class);
  private final KafkaOperatorTestBase kafkaLauncher = new KafkaOperatorTestBase();

  private static final String kafkaTopic = "adsdimensionstest";


  @Before
  public void beforeTest() throws Exception {
    kafkaLauncher.baseDir = "target/" + this.getClass().getName();
    FileUtils.deleteDirectory(new File(kafkaLauncher.baseDir));
    kafkaLauncher.startZookeeper();
    kafkaLauncher.startKafkaServer();
    kafkaLauncher.createTopic(kafkaTopic);
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
    conf.set("dt.operator.Kafka.prop.configProperties(metadata.broker.list)", "localhost:9092");
    conf.set("dt.operator.Kafka.prop.topic", kafkaTopic);
    conf.set("dt.operator.DimensionsComputation.attr.APPLICATION_WINDOW_COUNT", "1");

    lma.prepareDAG(new KafkaApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    CountDownLatch latch = new CountDownLatch(100);
    // Setup a message listener to receive the message
    KafkaTestConsumer listener = new KafkaTestConsumer(kafkaTopic);

    listener.setLatch(latch);
    new Thread(listener).start();

    latch.await(30, TimeUnit.SECONDS);
    lc.shutdown();

    String lastMessage;
    Assert.assertTrue("Minimum messages received from Kafka " + listener.holdingBuffer, listener.holdingBuffer.size() >= 100);
    while (!listener.holdingBuffer.isEmpty()) {
      lastMessage = listener.getMessage(listener.holdingBuffer.poll());
      Assert.assertNotNull("Did not receive message from Kafka", lastMessage);
      LOG.debug("received:\n{}", lastMessage);
    }

  }

}