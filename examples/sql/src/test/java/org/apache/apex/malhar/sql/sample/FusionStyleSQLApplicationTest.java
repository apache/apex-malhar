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
package org.apache.apex.malhar.sql.sample;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.apex.malhar.kafka.EmbeddedKafka;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

public class FusionStyleSQLApplicationTest
{
  private final String testTopicData = "dataTopic";
  private final String testTopicResult = "resultTopic";

  private TimeZone defaultTZ;
  private EmbeddedKafka kafka;

  private static String outputFolder = "target/output/";

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception
  {
    defaultTZ = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

    kafka = new EmbeddedKafka();
    kafka.start();
    kafka.createTopic(testTopicData);
    kafka.createTopic(testTopicResult);

    outputFolder += testName.getMethodName() + "/";
  }

  @After
  public void tearDown() throws Exception
  {
    kafka.stop();

    TimeZone.setDefault(defaultTZ);
  }

  @Test
  public void test() throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties-FusionStyleSQLApplication.xml"));

      conf.set("dt.operator.KafkaInput.prop.topics", testTopicData);
      conf.set("dt.operator.KafkaInput.prop.clusters", kafka.getBroker());
      conf.set("folderPath", outputFolder);
      conf.set("fileName", "out.tmp");

      FusionStyleSQLApplication app = new FusionStyleSQLApplication();

      lma.prepareDAG(app, conf);

      LocalMode.Controller lc = lma.getController();

      lc.runAsync();
      kafka.publish(testTopicData, Arrays.asList("15/02/2016 10:15:00 +0000,1,paint1,11",
          "15/02/2016 10:16:00 +0000,2,paint2,12",
          "15/02/2016 10:17:00 +0000,3,paint3,13", "15/02/2016 10:18:00 +0000,4,paint4,14",
          "15/02/2016 10:19:00 +0000,5,paint5,15", "15/02/2016 10:10:00 +0000,6,abcde6,16"));

      Assert.assertTrue(PureStyleSQLApplicationTest.waitTillFileIsPopulated(outputFolder, 40000));
      lc.shutdown();

      File file = new File(outputFolder);
      File file1 = new File(outputFolder + file.list()[0]);
      List<String> strings = FileUtils.readLines(file1);

      String[] actualLines = strings.toArray(new String[strings.size()]);
      String[] expectedLines = new String[] {
          "15/02/2016 10:18:00 +0000,15/02/2016 12:00:00 +0000,OILPAINT4",
          "",
          "15/02/2016 10:19:00 +0000,15/02/2016 12:00:00 +0000,OILPAINT5",
          ""};
      Assert.assertEquals(expectedLines.length, actualLines.length);
      for (int i = 0; i < actualLines.length; i++) {
        Assert.assertEquals(expectedLines[i], actualLines[i]);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
