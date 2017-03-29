/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import java.util.ArrayList;

import javax.validation.ConstraintViolationException;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.hadoop.conf.Configuration;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.batey.kafka.unit.KafkaUnitRule;
import info.batey.kafka.unit.KafkaUnit;

import kafka.producer.KeyedMessage;

import com.datatorrent.api.LocalMode;

import static org.junit.Assert.assertTrue;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
  private static final String TOPIC = "kafka2hdfs";

  private static final int zkPort = 2181;
  private static final int  brokerPort = 9092;
  private static final String BROKER = "localhost:" + brokerPort;
  private static final String FILE_NAME = "test";
  private static final String FILE_DIR  = "/tmp/FromKafka";
  private static final String FILE_PATH = FILE_DIR + "/" + FILE_NAME + ".0";     // first part

  // test messages
  private static String[] lines =
  {
    "1st line",
    "2nd line",
    "3rd line",
    "4th line",
    "5th line",
  };

  // broker port must match properties.xml
  @Rule
  public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(zkPort, brokerPort);

  @Test
  public void testApplication() throws Exception {
    try {
      // delete output file if it exists
      File file = new File(FILE_PATH);
      file.delete();

      // write messages to Kafka topic
      writeToTopic();

      // run app asynchronously; terminate after results are checked
      LocalMode.Controller lc = asyncRun();

      // check for presence of output file
      chkOutput();

      // compare output lines to input
      compare();

      lc.shutdown();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private void writeToTopic() {
    KafkaUnit ku = kafkaUnitRule.getKafkaUnit();
    ku.createTopic(TOPIC);
    for (String line : lines) {
      KeyedMessage<String, String> kMsg = new KeyedMessage<>(TOPIC, line);
      ku.sendMessages(kMsg);
    }
    LOG.debug("Sent messages to topic {}", TOPIC);
  }

  private Configuration getConfig() {
      Configuration conf = new Configuration(false);
      String pre = "dt.operator.kafkaIn.prop.";
      conf.setEnum(pre + "initialOffset",
                   AbstractKafkaInputOperator.InitialOffset.EARLIEST);
      conf.setInt(pre + "initialPartitionCount", 1);
      conf.set(   pre + "topics",                TOPIC);
      conf.set(   pre + "clusters",              BROKER);

      pre = "dt.operator.fileOut.prop.";
      conf.set(   pre + "filePath",        FILE_DIR);
      conf.set(   pre + "baseName",        FILE_NAME);
      conf.setInt(pre + "maxLength",       40);
      conf.setInt(pre + "rotationWindows", 3);

      return conf;
  }

  private LocalMode.Controller asyncRun() throws Exception {
    Configuration conf = getConfig();
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(new KafkaApp(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    return lc;
  }

  private static void chkOutput() throws Exception {
    File file = new File(FILE_PATH);
    final int MAX = 60;
    for (int i = 0; i < MAX && (! file.exists()); ++i ) {
      LOG.debug("Sleeping, i = {}", i);
      Thread.sleep(1000);
    }
    if (! file.exists()) {
      String msg = String.format("Error: %s not found after %d seconds%n", FILE_PATH, MAX);
      throw new RuntimeException(msg);
    }
  }

  private static void compare() throws Exception {
    // read output file
    File file = new File(FILE_PATH);
    BufferedReader br = new BufferedReader(new FileReader(file));
    ArrayList<String> list = new ArrayList<>();
    String line;
    while (null != (line = br.readLine())) {
      list.add(line);
    }
    br.close();

    // compare
    Assert.assertEquals("number of lines", list.size(), lines.length);
    for (int i = 0; i < lines.length; ++i) {
      assertTrue("line", lines[i].equals(list.get(i)));
    }
  }
}
