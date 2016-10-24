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
import java.io.IOException;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import com.datatorrent.api.LocalMode;


public class PureStyleSQLApplicationTest
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
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties-PureStyleSQLApplication.xml"));

    conf.set("broker", kafka.getBroker());
    conf.set("topic", testTopicData);
    conf.set("outputFolder", outputFolder);
    conf.set("destFileName", "out.tmp");

    PureStyleSQLApplication app = new PureStyleSQLApplication();

    lma.prepareDAG(app, conf);

    LocalMode.Controller lc = lma.getController();

    lc.runAsync();
    kafka.publish(testTopicData, Arrays.asList(
        "15/02/2016 10:15:00 +0000,1,paint1,11",
        "15/02/2016 10:16:00 +0000,2,paint2,12",
        "15/02/2016 10:17:00 +0000,3,paint3,13",
        "15/02/2016 10:18:00 +0000,4,paint4,14",
        "15/02/2016 10:19:00 +0000,5,paint5,15",
        "15/02/2016 10:10:00 +0000,6,abcde6,16"));

    Assert.assertTrue(waitTillFileIsPopulated(outputFolder, 40000));
    lc.shutdown();

    File file = new File(outputFolder);
    File file1 = new File(outputFolder + file.list()[0]);
    List<String> strings = FileUtils.readLines(file1);

    String[] actualLines = strings.toArray(new String[strings.size()]);

    String[] expectedLines = new String[]{
        "15/02/2016 10:18:00 +0000,15/02/2016 12:00:00 +0000,OILPAINT4",
        "",
        "15/02/2016 10:19:00 +0000,15/02/2016 12:00:00 +0000,OILPAINT5",
        ""};

    Assert.assertEquals(expectedLines.length, actualLines.length);
    for (int i = 0;i < expectedLines.length; i++) {
      Assert.assertEquals(expectedLines[i], actualLines[i]);
    }
  }

  public static boolean waitTillFileIsPopulated(String outputFolder, int timeout) throws IOException, InterruptedException
  {
    boolean result;
    long now = System.currentTimeMillis();
    Path outDir = new Path("file://" + new File(outputFolder).getAbsolutePath());
    try (FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration())) {
      List<String> strings = Lists.newArrayList();
      while (System.currentTimeMillis() - now < timeout) {
        if (fs.exists(outDir)) {
          File file = new File(outputFolder);
          if (file.list().length > 0) {
            File file1 = new File(outputFolder + file.list()[0]);
            strings = FileUtils.readLines(file1);
            if (strings.size() != 0) {
              break;
            }
          }
        }

        Thread.sleep(500);
      }

      result = fs.exists(outDir) && (strings.size() != 0);
    }

    return result;
  }

}
