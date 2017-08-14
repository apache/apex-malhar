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
package org.apache.apex.malhar.contrib.geode;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.google.common.collect.Maps;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;

/**
 * Test setup instructions
 *
 * Setup a local Geode cluster by starting locator & server using Geode's Gfsh shell
 *
 * start Gfsh - gemfire-assembly/build/install/apache-geode/bin/gfsh
 *
 * gfsh>start locator - gfsh>start locator --name=L1
 *
 * gfsh>start server - gfsh>start server --name=S1
 *
 * Checkpointing storage agent needs to dynamically create Geode regions for per application.
 *
 * To be able Programmatically create Geode region deploy below Server function through Gfsh
 *
 * > jar -cvf geode-fun.jar com/datatorrent/contrib/geode/RegionCreateFunction.class
 *
 * gfsh> deploy --jar=/tmp/jars/geode-fun.jar
 *
 * gfsh> list functions // verify RegionCreateFunction is listed
 *
 * gfsh> describe member --name=L1
 *
 * provide locators details from above command in LOCATOR_HOST as <>locator-host:<locator-io>
 */
public class  GeodeKeyValueStorageAgentTest

{
  private static class TestMeta extends TestWatcher
  {
    String applicationPath;
    GeodeKeyValueStorageAgent storageAgent;
    static String LOCATOR_HOST = "localhost:10334";
    static final String REGION_NAME = "GeodeKeyValueStorageAgentTest";

    @Override
    protected void starting(Description description)
    {
      super.starting(description);

      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      if (System.getProperty("dev.locator.connection") != null) {
        LOCATOR_HOST = System.getProperty("dev.locator.connection");
      }
      try {
        FileUtils.forceMkdir(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      Configuration config = new Configuration();
      config.set(GeodeKeyValueStorageAgent.GEODE_LOCATOR_STRING, LOCATOR_HOST);

      storageAgent = new GeodeKeyValueStorageAgent(config);
      storageAgent.setApplicationId(REGION_NAME);
      Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_PATH, applicationPath);
    }

    @Override
    protected void finished(Description description)
    {
      try {
        storageAgent.getStore().disconnect();
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testSave() throws IOException
  {
    Map<Integer, String> data = Maps.newHashMap();
    data.put(1, "one");
    data.put(2, "two");
    data.put(3, "three");
    testMeta.storageAgent.save(data, 1, 1);
    @SuppressWarnings("unchecked")
    Map<Integer, String> decoded = (Map<Integer, String>)testMeta.storageAgent.load(1, 1);
    Assert.assertEquals("dataOf1", data, decoded);
  }

  @Test
  public void testLoad() throws IOException
  {
    Map<Integer, String> dataOf1 = Maps.newHashMap();
    dataOf1.put(1, "one");
    dataOf1.put(2, "two");
    dataOf1.put(3, "three");

    Map<Integer, String> dataOf2 = Maps.newHashMap();
    dataOf2.put(4, "four");
    dataOf2.put(5, "five");
    dataOf2.put(6, "six");

    testMeta.storageAgent.save(dataOf1, 1, 1);
    testMeta.storageAgent.save(dataOf2, 2, 1);
    @SuppressWarnings("unchecked")
    Map<Integer, String> decoded1 = (Map<Integer, String>)testMeta.storageAgent.load(1, 1);

    @SuppressWarnings("unchecked")
    Map<Integer, String> decoded2 = (Map<Integer, String>)testMeta.storageAgent.load(2, 1);
    Assert.assertEquals("data of 1", dataOf1, decoded1);
    Assert.assertEquals("data of 2", dataOf2, decoded2);
  }

  @Test
  public void testRecovery() throws IOException
  {
    testSave();
    Configuration config = new Configuration();
    config.set(GeodeKeyValueStorageAgent.GEODE_LOCATOR_STRING, testMeta.LOCATOR_HOST);
    testMeta.storageAgent = new GeodeKeyValueStorageAgent(config);
    testMeta.storageAgent.setApplicationId(testMeta.REGION_NAME);
    testSave();
  }

  @Test
  public void testDelete() throws IOException, FunctionDomainException, TypeMismatchException, NameResolutionException,
    QueryInvocationTargetException
  {
    testLoad();

    testMeta.storageAgent.delete(1, 1);
    Assert.assertTrue("operator 2 window 1", (testMeta.storageAgent.load(2, 1) != null));
    Assert.assertFalse("operator 1 window 1", (testMeta.storageAgent.load(1, 1) != null));
  }

  //@Test
  public void testGetWindowIds() throws IOException
  {
    final String REGION_NAME = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date());
    testMeta.storageAgent.setApplicationId(REGION_NAME);
    Map<Integer, String> obj = Maps.newHashMap();
    obj.put(1, "one");
    obj.put(2, "two");
    obj.put(3, "three");

    long[] op1WindowIds = {111, 112, 113};
    for (long l : op1WindowIds) {
      testMeta.storageAgent.save(obj, 1, l);
    }

    long[] op2WindowIds = {211, 212};
    for (long l : op2WindowIds) {
      testMeta.storageAgent.save(obj, 2, l);
    }

    Arrays.sort(op1WindowIds);
    Arrays.sort(op2WindowIds);
    long[] op1WinIds = testMeta.storageAgent.getWindowIds(1);
    long[] op2WinIds = testMeta.storageAgent.getWindowIds(2);
    Arrays.sort(op1WinIds);
    Arrays.sort(op2WinIds);

    Assert.assertEquals(op1WindowIds.length, op1WinIds.length);
    Assert.assertEquals(op2WindowIds.length, op2WinIds.length);

    Assert.assertTrue(Arrays.equals(op1WindowIds, op1WinIds));
    Assert.assertTrue(Arrays.equals(op2WindowIds, op2WinIds));

  }

}
