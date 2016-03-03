package com.example.myapexapp;

import java.io.File;

import javax.validation.ConstraintViolationException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.LocalMode;
import com.datatorrent.contrib.kafka.KafkaOperatorTestBase;
import com.datatorrent.contrib.kafka.KafkaTestProducer;

/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
public class AtomicFileOutputAppTest
{
  private final KafkaOperatorTestBase kafkaLauncher = new KafkaOperatorTestBase();
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
  private static final String KAFKA_TOPIC = "exactly-once-test";
  private static final String TARGET_DIR = "target/atomicFileOutput";

  @Before
  public void beforeTest() throws Exception {
    kafkaLauncher.baseDir = "target/" + this.getClass().getName();
    FileUtils.deleteDirectory(new File(kafkaLauncher.baseDir));
    kafkaLauncher.startZookeeper();
    kafkaLauncher.startKafkaServer();
    kafkaLauncher.createTopic(0, KAFKA_TOPIC);
  }

  @After
  public void afterTest() {
    kafkaLauncher.stopKafkaServer();
    kafkaLauncher.stopZookeeper();
  }

  @Test
  public void testApplication() throws Exception {
    try {

      File targetDir = new File(TARGET_DIR);
      FileUtils.deleteDirectory(targetDir);
      FileUtils.forceMkdir(targetDir);

      // produce some test data
      KafkaTestProducer p = new KafkaTestProducer(KAFKA_TOPIC);
      String[] words = "count the words from kafka and store them in the db".split("\\s+");
      p.setMessages(Lists.newArrayList(words));
      new Thread(p).start();

      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      conf.set("dt.operator.kafkaInput.prop.topic", KAFKA_TOPIC);
      conf.set("dt.operator.kafkaInput.prop.zookeeper", "localhost:" + KafkaOperatorTestBase.TEST_ZOOKEEPER_PORT[0]);
      conf.set("dt.operator.kafkaInput.prop.maxTuplesPerWindow", "1"); // consume one word per window
      conf.set("dt.operator.fileWriter.prop.filePath", TARGET_DIR);

      lma.prepareDAG(new AtomicFileOutputApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync(); // test will terminate after results are available

      long timeout = System.currentTimeMillis() + 60000; // 60s timeout

      File outputFile = new File(TARGET_DIR, AtomicFileOutputApp.FileWriter.FILE_NAME_PREFIX);
      while (!outputFile.exists() && timeout > System.currentTimeMillis()) {
        Thread.sleep(1000);
        LOG.debug("Waiting for {}", outputFile);
      }

      Assert.assertTrue("output file exists " + AtomicFileOutputApp.FileWriter.FILE_NAME_PREFIX, outputFile.exists() &&
          outputFile.isFile());

      lc.shutdown();

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
