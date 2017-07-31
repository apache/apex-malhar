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
package org.apache.apex.examples.kafka.hdfs2kafka;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

import com.datatorrent.api.LocalMode;

import info.batey.kafka.unit.KafkaUnit;
import info.batey.kafka.unit.KafkaUnitRule;

import static org.junit.Assert.assertTrue;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
  private static final String TOPIC = "hdfs2kafka";
  private static final String directory = "target/hdfs2kafka";
  private static final String FILE_NAME = "messages.txt";

  private static final int zkPort = NetUtils.getFreeSocketPort();
  private static final int brokerPort = NetUtils.getFreeSocketPort();
  private static final String BROKER = "localhost:" + brokerPort;
  //private static final String FILE_PATH = FILE_DIR + "/" + FILE_NAME + ".0";     // first part

  // test messages
  private static String[] lines = {
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
  public void testApplication() throws IOException, Exception
  {
    try {
      // create file in monitored HDFS directory
      createFile();

      // run app asynchronously; terminate after results are checked
      LocalMode.Controller lc = asyncRun();

      // get messages from Kafka topic and compare with input
      chkOutput();

      lc.shutdown();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  // create a file with content from 'lines'
  private void createFile() throws IOException
  {
    // remove old file and create new one
    File file = new File(directory, FILE_NAME);
    FileUtils.deleteQuietly(file);
    try {
      String data = StringUtils.join(lines, "\n") + "\n";    // add final newline
      FileUtils.writeStringToFile(file, data, "UTF-8");
    } catch (IOException e) {
      LOG.error("Error: Failed to create file {} in {}", FILE_NAME, directory);
      e.printStackTrace();
    }
    LOG.debug("Created file {} with {} lines in {}", FILE_NAME, lines.length, directory);
  }

  private LocalMode.Controller asyncRun() throws Exception
  {
    Configuration conf = getConfig();
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(new Application(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    return lc;
  }

  private Configuration getConfig()
  {
    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties-hdfs2kafka.xml"));
    conf.set("dt.operator.lines.prop.directory", directory);
    conf.set("dt.operator.kafkaOutput.prop.producerProperties",
        "serializer.class=kafka.serializer.StringEncoder,producer.type=async,metadata.broker.list=" + BROKER);

    return conf;
  }

  private void chkOutput() throws Exception
  {
    KafkaUnit ku = kafkaUnitRule.getKafkaUnit();
    List<String> messages = null;

    // wait for messages to appear in kafka
    Thread.sleep(10000);

    try {
      messages = ku.readMessages(TOPIC, lines.length);
    } catch (Exception e) {
      LOG.error("Error: Got exception {}", e);
    }

    int i = 0;
    for (String msg : messages) {
      assertTrue("Error: message mismatch", msg.equals(lines[i]));
      ++i;
    }
  }

}
