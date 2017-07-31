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
package org.apache.apex.examples.exactlyonce;

import java.io.File;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.api.Launcher.AppHandle;
import org.apache.apex.api.Launcher.LaunchMode;
import org.apache.apex.api.Launcher.ShutdownMode;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

import com.datatorrent.api.Attribute;

import info.batey.kafka.unit.KafkaUnit;
import info.batey.kafka.unit.KafkaUnitRule;
import kafka.producer.KeyedMessage;

public class ExactlyOnceFileOutputAppTest
{
  private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceJdbcOutputTest.class);
  private static final String TARGET_DIR = "target/atomicFileOutput";

  private final int brokerPort = NetUtils.getFreeSocketPort();
  private final int zkPort = NetUtils.getFreeSocketPort();

  @Rule
  public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(zkPort, brokerPort);

  {
    // required to avoid 50 partitions auto creation
    this.kafkaUnitRule.getKafkaUnit().setKafkaBrokerConfig("num.partitions", "1");
    this.kafkaUnitRule.getKafkaUnit().setKafkaBrokerConfig("offsets.topic.num.partitions", "1");
  }

  @Test
  public void testApplication() throws Exception
  {
    File targetDir = new File(TARGET_DIR);
    FileUtils.deleteDirectory(targetDir);
    FileUtils.forceMkdir(targetDir);

    KafkaUnit ku = kafkaUnitRule.getKafkaUnit();
    String topicName = "testTopic";
    // topic creation is async and the producer may also auto-create it
    ku.createTopic(topicName, 1);

    // produce test data
    String[] words = "count count the words from kafka and store them in a file".split("\\s+");
    for (String word : words) {
      ku.sendMessages(new KeyedMessage<String, String>(topicName, word));
    }

    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    conf.set("apex.operator.kafkaInput.prop.topics", topicName);
    conf.set("apex.operator.kafkaInput.prop.clusters", "localhost:" + brokerPort);
    conf.set("apex.operator.kafkaInput.prop.maxTuplesPerWindow", "2"); // consume one word per window
    conf.set("apex.operator.kafkaInput.prop.initialOffset", "EARLIEST");
    conf.set("apex.operator.fileWriter.prop.filePath", TARGET_DIR);

    EmbeddedAppLauncher<?> launcher = Launcher.getLauncher(LaunchMode.EMBEDDED);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    launchAttributes.put(EmbeddedAppLauncher.RUN_ASYNC, true); // terminate after results are available
    AppHandle appHandle = launcher.launchApp(new ExactlyOnceFileOutputApp(), conf, launchAttributes);

    long timeout = System.currentTimeMillis() + 60000; // 60s timeout

    File outputFile = new File(TARGET_DIR, ExactlyOnceFileOutputApp.FileWriter.FILE_NAME_PREFIX);
    while (!outputFile.exists() && timeout > System.currentTimeMillis()) {
      Thread.sleep(1000);
      LOG.debug("Waiting for {}", outputFile);
    }

    Assert.assertTrue("output file exists " + ExactlyOnceFileOutputApp.FileWriter.FILE_NAME_PREFIX, outputFile.exists() &&
        outputFile.isFile());

    String result = FileUtils.readFileToString(outputFile);
    Assert.assertTrue(result.contains("count=2"));

    appHandle.shutdown(ShutdownMode.KILL);
  }

}
