/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

/**
 * Tests for {@link IdempotentStorageManager}
 */
public class IdempotentStorageManagerTest
{
  private static class TestMeta extends TestWatcher
  {

    String applicationPath;
    IdempotentStorageManager.FSIdempotentStorageManager storageManager;
    Context.OperatorContext context;

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      storageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();

      Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_PATH, applicationPath);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);

      storageManager.setup(context);
    }

    @Override
    protected void finished(Description description)
    {
      storageManager.teardown();
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testLargestRecoveryWindow()
  {
    Assert.assertEquals("largest recovery", Stateless.WINDOW_ID, testMeta.storageManager.getLargestRecoveryWindow());
  }

  @Test
  public void testSave() throws IOException
  {
    Map<Integer, String> data = Maps.newHashMap();
    data.put(1, "one");
    data.put(2, "two");
    data.put(3, "three");
    testMeta.storageManager.save(data, 1, 1);
    testMeta.storageManager.setup(testMeta.context);
    @SuppressWarnings("unchecked")
    Map<Integer, String> decoded = (Map<Integer, String>) testMeta.storageManager.load(1, 1);
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

    testMeta.storageManager.save(dataOf1, 1, 1);
    testMeta.storageManager.save(dataOf2, 2, 1);
    testMeta.storageManager.setup(testMeta.context);
    Map<Integer, Object> decodedStates = testMeta.storageManager.load(1);
    Assert.assertEquals("no of states", 2, decodedStates.size());
    for (Integer operatorId : decodedStates.keySet()) {
      if (operatorId == 1) {
        Assert.assertEquals("data of 1", dataOf1, decodedStates.get(1));
      }
      else {
        Assert.assertEquals("data of 2", dataOf2, decodedStates.get(2));
      }
    }
  }

  @Test
  public void testRecovery() throws IOException
  {
    Map<Integer, String> dataOf1 = Maps.newHashMap();
    dataOf1.put(1, "one");
    dataOf1.put(2, "two");
    dataOf1.put(3, "three");

    Map<Integer, String> dataOf2 = Maps.newHashMap();
    dataOf2.put(4, "four");
    dataOf2.put(5, "five");
    dataOf2.put(6, "six");

    testMeta.storageManager.save(dataOf1, 1, 1);
    testMeta.storageManager.save(dataOf2, 2, 2);

    testMeta.storageManager.setup(testMeta.context);
    Assert.assertEquals("largest recovery window", 2, testMeta.storageManager.getLargestRecoveryWindow());
  }

  @Test
  public void testDelete() throws IOException
  {
    Map<Integer, String> dataOf1 = Maps.newHashMap();
    dataOf1.put(1, "one");
    dataOf1.put(2, "two");
    dataOf1.put(3, "three");

    Map<Integer, String> dataOf2 = Maps.newHashMap();
    dataOf2.put(4, "four");
    dataOf2.put(5, "five");
    dataOf2.put(6, "six");

    Map<Integer, String> dataOf3 = Maps.newHashMap();
    dataOf2.put(7, "seven");
    dataOf2.put(8, "eight");
    dataOf2.put(9, "nine");

    testMeta.storageManager.save(dataOf1, 1, 1);
    testMeta.storageManager.save(dataOf2, 2, 1);
    testMeta.storageManager.save(dataOf3, 3, 1);

    testMeta.storageManager.partitioned(Lists.<IdempotentStorageManager>newArrayList(testMeta.storageManager),
      Sets.newHashSet(2, 3));
    testMeta.storageManager.setup(testMeta.context);
    testMeta.storageManager.deleteUpTo(1, 1);

    Path appPath = new Path(testMeta.applicationPath + '/' + testMeta.storageManager.recoveryPath);
    FileSystem fs = FileSystem.newInstance(appPath.toUri(), new Configuration());
    Assert.assertEquals("no data for 1", 0, fs.listStatus(new Path(appPath, Integer.toString(1))).length);
    Assert.assertEquals("no data for 2", false, fs.exists(new Path(appPath, Integer.toString(2))));
    Assert.assertEquals("no data for 3", false, fs.exists(new Path(appPath, Integer.toString(3))));
  }

}
