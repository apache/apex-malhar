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
package com.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

import info.batey.kafka.unit.KafkaUnit;
import info.batey.kafka.unit.KafkaUnitRule;

import static org.junit.Assert.fail;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{
  private static final String directory = "target/kafka_exactly";
  private String tuplesUntilKill;

  private static final int zkPort = 2181;
  private static final int brokerPort = 9092;

  private static final Logger logger = LoggerFactory.getLogger(ApplicationTest.class);

  // broker port must match properties.xml
  @Rule
  public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(zkPort, brokerPort);

  //remove '@After' to keep validation output file
  @Before
  @After
  public void cleanup()
  {
    FileUtils.deleteQuietly(new File(directory));
  }

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      createTopics();

      // run app asynchronously; terminate after results are checked
      Configuration conf = getConfig();
      LocalMode lma = LocalMode.newInstance();
      lma.prepareDAG(new Application(), conf);
      ValidationToFile validationToFile = (ValidationToFile)lma.getDAG().getOperatorMeta("validationToFile").getOperator();
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      // get messages from Kafka topic and compare with input
      int count = 1;
      int maxSleepRounds = 300;
      while (!validationToFile.validationDone) {
        logger.info("Sleeping ....");
        Thread.sleep(500);
        if(count > maxSleepRounds){
          fail("validationDone flag did not get set to true in ValidationToFile operator");
        }
        count++;
      }
      lc.shutdown();
      Thread.sleep(1000);
      checkOutput();
    } catch (ConstraintViolationException e) {
      fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private Configuration getConfig()
  {
    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    conf.set("dt.application.KafkaExactlyOnce.operator.passthrough.prop.directoryPath", directory);
    conf.set("dt.application.KafkaExactlyOnce.operator.validationToFile.prop.filePath", directory);
    tuplesUntilKill = conf.get("dt.application.KafkaExactlyOnce.operator.passthrough.prop.tuplesUntilKill");
    return conf;
  }

  private void createTopics() throws Exception
  {
    KafkaUnit ku = kafkaUnitRule.getKafkaUnit();
    ku.createTopic("exactly-once");
    ku.createTopic("at-least-once");
  }

  private void checkOutput() throws IOException
  {
    String validationOutput;
    File folder = new File(directory);

    FilenameFilter filenameFilter = new FilenameFilter()
    {
      @Override
      public boolean accept(File dir, String name)
      {
        if (name.split("_")[0].equals("validation.txt")) {
          return true;
        }
        return false;
      }
    };
    File validationFile = folder.listFiles(filenameFilter)[0];
    try (FileInputStream inputStream = new FileInputStream(validationFile)) {
      validationOutput = IOUtils.toString(inputStream);
      logger.info("Validation output: {}", validationOutput);
    }

    Assert.assertTrue(validationOutput.contains("exactly-once: 0"));

    //assert works only for tuplesUntilKill values low enough to kill operator before checkpointing
    Assert.assertEquals("Duplicates: exactly-once: 0, at-least-once: " + tuplesUntilKill, validationOutput);
  }
}
