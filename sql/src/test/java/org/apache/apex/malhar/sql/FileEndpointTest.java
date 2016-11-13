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
package org.apache.apex.malhar.sql;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import javax.validation.ConstraintViolationException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.apex.malhar.sql.table.CSVMessageFormat;
import org.apache.apex.malhar.sql.table.FileEndpoint;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

public class FileEndpointTest
{
  private TimeZone defaultTZ;
  private static String outputFolder = "target/output/";

  @Rule
  public TestName testName = new TestName();

  public static String apex_concat_str(String s1, String s2)
  {
    return s1 + s2;
  }

  @Before
  public void setUp() throws Exception
  {
    defaultTZ = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

    outputFolder += testName.getMethodName() + "/";
  }

  @After
  public void tearDown() throws Exception
  {
    TimeZone.setDefault(defaultTZ);
  }

  @Test
  public void testApplication() throws Exception
  {
    File modelFile = new File("src/test/resources/model/model_file_csv.json");
    String model = FileUtils.readFileToString(modelFile);

    PrintStream originalSysout = System.out;

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));

    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Application(model), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      waitTillStdoutIsPopulated(baos, 30000);

      lc.shutdown();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    } catch (Exception e) {
      Assert.fail("Exception: " + e);
    }

    System.setOut(originalSysout);

    String[] sout = baos.toString().split(System.lineSeparator());
    Collection<String> filter = Collections2.filter(Arrays.asList(sout), Predicates.containsPattern("Delta Record:"));

    String[] actualLines = filter.toArray(new String[filter.size()]);
    Assert.assertEquals(6, actualLines.length);
    Assert.assertTrue(actualLines[0].contains("RowTime=Mon Feb 15 10:15:00 GMT 2016, Product=paint1"));
    Assert.assertTrue(actualLines[1].contains("RowTime=Mon Feb 15 10:16:00 GMT 2016, Product=paint2"));
    Assert.assertTrue(actualLines[2].contains("RowTime=Mon Feb 15 10:17:00 GMT 2016, Product=paint3"));
    Assert.assertTrue(actualLines[3].contains("RowTime=Mon Feb 15 10:18:00 GMT 2016, Product=paint4"));
    Assert.assertTrue(actualLines[4].contains("RowTime=Mon Feb 15 10:19:00 GMT 2016, Product=paint5"));
    Assert.assertTrue(actualLines[5].contains("RowTime=Mon Feb 15 10:10:00 GMT 2016, Product=abcde6"));
  }

  private boolean waitTillStdoutIsPopulated(ByteArrayOutputStream baos, int timeout) throws InterruptedException,
    IOException
  {
    long now = System.currentTimeMillis();
    Collection<String> filter = Lists.newArrayList();
    while (System.currentTimeMillis() - now < timeout) {
      baos.flush();
      String[] sout = baos.toString().split(System.lineSeparator());
      filter = Collections2.filter(Arrays.asList(sout), Predicates.containsPattern("Delta Record:"));
      if (filter.size() != 0) {
        break;
      }

      Thread.sleep(500);
    }

    return (filter.size() != 0);
  }

  @Test
  public void testApplicationSelectInsertWithAPI() throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new ApplicationSelectInsertWithAPI(), conf);

      LocalMode.Controller lc = lma.getController();
      lc.runAsync();
      /**
       * Wait time is 40 sec to ensure that checkpoint happens. AbstractFileOutputOperators flushes the stream
       * in beforeCheckpoint call.
       */
      Assert.assertTrue(waitTillFileIsPopulated(outputFolder, 40000));
      lc.shutdown();
    } catch (Exception e) {
      Assert.fail("constraint violations: " + e);
    }

    File file = new File(outputFolder);
    File file1 = new File(outputFolder + file.list()[0]);
    List<String> strings = FileUtils.readLines(file1);

    String[] actualLines = strings.toArray(new String[strings.size()]);

    String[] expectedLines = new String[]{"15/02/2016 10:18:00 +0000,15/02/2016 00:00:00 +0000,OILPAINT4", "",
      "15/02/2016 10:19:00 +0000,15/02/2016 00:00:00 +0000,OILPAINT5", ""};
    Assert.assertTrue(Arrays.deepEquals(actualLines, expectedLines));
  }

  private boolean waitTillFileIsPopulated(String outputFolder, int timeout) throws IOException, InterruptedException
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


  public static class Application implements StreamingApplication
  {
    String model;

    public Application(String model)
    {
      this.model = model;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      SQLExecEnvironment.getEnvironment()
          .withModel(model)
          .executeSQL(dag, "SELECT STREAM ROWTIME, PRODUCT FROM ORDERS");
    }
  }

  public static class ApplicationSelectInsertWithAPI implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      String schemaIn = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
          "{\"name\":\"RowTime\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy HH:mm:ss Z\"}}," +
          "{\"name\":\"id\",\"type\":\"Integer\"}," +
          "{\"name\":\"Product\",\"type\":\"String\"}," +
          "{\"name\":\"units\",\"type\":\"Integer\"}]}";
      String schemaOut = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
          "{\"name\":\"RowTime1\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy HH:mm:ss Z\"}}," +
          "{\"name\":\"RowTime2\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy HH:mm:ss Z\"}}," +
          "{\"name\":\"Product\",\"type\":\"String\"}]}";

      SQLExecEnvironment.getEnvironment()
          .registerTable("ORDERS", new FileEndpoint("src/test/resources/input.csv",
          new CSVMessageFormat(schemaIn)))
          .registerTable("SALES", new FileEndpoint(outputFolder, "out.tmp", new CSVMessageFormat(schemaOut)))
          .registerFunction("APEXCONCAT", FileEndpointTest.class, "apex_concat_str")
          .executeSQL(dag, "INSERT INTO SALES " + "SELECT STREAM ROWTIME, " + "FLOOR(ROWTIME TO DAY), " +
          "APEXCONCAT('OILPAINT', SUBSTRING(PRODUCT, 6, 7)) " + "FROM ORDERS WHERE ID > 3 " + "AND " +
          "PRODUCT LIKE 'paint%'");
    }
  }

}
