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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

import info.batey.kafka.unit.KafkaUnit;
import info.batey.kafka.unit.KafkaUnitRule;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{
  private static final String directory = "target/kafka_exactly";
  private String tuplesUntilKill;

  private static final int zkPort = 2181;
  private static final int brokerPort = 9092;

  // broker port must match properties.xml
  @Rule
  public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(zkPort, brokerPort);

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      cleanup();
      createTopics();

      // run app asynchronously; terminate after results are checked
      Configuration conf = getConfig();
      LocalMode lma = LocalMode.newInstance();
      lma.prepareDAG(new Application(), conf);
      ValidationToFile validationToFile = (ValidationToFile)lma.getDAG().getOperatorMeta("validationToFile").getOperator();
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      // get messages from Kafka topic and compare with input
      while(!validationToFile.validationDone)
      {
        System.out.println("Sleeping ....");
        Thread.sleep(500);
      }
      lc.shutdown();
      Thread.sleep(1000);
      checkOutput();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private void cleanup()
  {
    try {
      FileUtils.deleteDirectory(new File(directory));
    } catch (IOException e) {
      throw new RuntimeException(e);
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

    FileInputStream inputStream = new FileInputStream(validationFile);
    try {
      validationOutput = IOUtils.toString(inputStream);
      System.out.println("Validation output: " + validationOutput);
    } finally {
      inputStream.close();
    }

    Assert.assertTrue(validationOutput.contains("exactly-once: 0"));

    //assert works only for tuplesUntilKill values low enough to kill operator before checkpointing
    Assert.assertEquals("Duplicates: exactly-once: 0, at-least-once: " + tuplesUntilKill, validationOutput);
  }
}
