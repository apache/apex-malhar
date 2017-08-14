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
package org.apache.apex.malhar.lib.wal;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.util.TestUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.Pair;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

/**
 * Tests for {@link WindowDataManager}
 */
public class FSWindowDataManagerTest
{
  private static class TestMeta extends TestWatcher
  {

    String applicationPath;
    Attribute.AttributeMap.DefaultAttributeMap attributes;

    @Override
    protected void starting(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
      super.starting(description);
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();

      attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_PATH, applicationPath);
    }

    @Override
    protected void finished(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testLargestRecoveryWindow()
  {
    Pair<Context.OperatorContext, FSWindowDataManager> pair = createManagerAndContextFor(1);
    pair.second.setup(pair.first);
    Assert.assertEquals("largest recovery", Stateless.WINDOW_ID, pair.second.getLargestCompletedWindow());
    pair.second.teardown();
  }

  @Test
  public void testSave() throws IOException
  {
    Pair<Context.OperatorContext, FSWindowDataManager> pair = createManagerAndContextFor(1);
    pair.second.setup(pair.first);
    Map<Integer, String> data = Maps.newHashMap();
    data.put(1, "one");
    data.put(2, "two");
    data.put(3, "three");
    pair.second.save(data, 1);

    pair.second.setup(pair.first);
    @SuppressWarnings("unchecked")
    Map<Integer, String> artifact = (Map<Integer, String>)pair.second.retrieve(1);
    Assert.assertEquals("dataOf1", data, artifact);
    pair.second.teardown();
  }

  @Test
  public void testRetrieve() throws IOException
  {
    Pair<Context.OperatorContext, FSWindowDataManager> pair1 = createManagerAndContextFor(1);
    Pair<Context.OperatorContext, FSWindowDataManager> pair2 = createManagerAndContextFor(2);

    pair1.second.setup(pair1.first);
    pair2.second.setup(pair2.first);

    Map<Integer, String> dataOf1 = Maps.newHashMap();
    dataOf1.put(1, "one");
    dataOf1.put(2, "two");
    dataOf1.put(3, "three");

    Map<Integer, String> dataOf2 = Maps.newHashMap();
    dataOf2.put(4, "four");
    dataOf2.put(5, "five");
    dataOf2.put(6, "six");

    pair1.second.save(dataOf1, 1);
    pair2.second.save(dataOf2, 1);

    pair1.second.setup(pair1.first);
    Object artifact1 = pair1.second.retrieve(1);
    Assert.assertEquals("data of 1", dataOf1, artifact1);

    pair2.second.setup(pair2.first);
    Object artifact2 = pair2.second.retrieve(1);
    Assert.assertEquals("data of 2", dataOf2, artifact2);

    pair1.second.teardown();
    pair2.second.teardown();
  }

  @Test
  public void testRetrieveAllPartitions() throws IOException
  {
    Pair<Context.OperatorContext, FSWindowDataManager> pair1 = createManagerAndContextFor(1);
    Pair<Context.OperatorContext, FSWindowDataManager> pair2 = createManagerAndContextFor(2);

    pair1.second.setup(pair1.first);
    pair2.second.setup(pair2.first);

    Map<Integer, String> dataOf1 = Maps.newHashMap();
    dataOf1.put(1, "one");
    dataOf1.put(2, "two");
    dataOf1.put(3, "three");

    Map<Integer, String> dataOf2 = Maps.newHashMap();
    dataOf2.put(4, "four");
    dataOf2.put(5, "five");
    dataOf2.put(6, "six");

    pair1.second.save(dataOf1, 1);
    pair2.second.save(dataOf2, 1);

    pair1.second.teardown();
    pair2.second.teardown();

    List<WindowDataManager> managers = pair1.second.partition(3, null);

    managers.get(0).setup(pair1.first);
    Map<Integer, Object> artifacts = managers.get(0).retrieveAllPartitions(1);
    Assert.assertEquals("num artifacts", 2, artifacts.size());

    Assert.assertEquals("artifact 1", dataOf1, artifacts.get(1));
    Assert.assertEquals("artifact 2", dataOf2, artifacts.get(2));

    managers.get(0).teardown();
  }

  @Test
  public void testRecovery() throws IOException
  {
    Pair<Context.OperatorContext, FSWindowDataManager> pair1 = createManagerAndContextFor(1);
    Pair<Context.OperatorContext, FSWindowDataManager> pair2 = createManagerAndContextFor(2);

    pair1.second.setup(pair1.first);
    pair2.second.setup(pair2.first);

    Map<Integer, String> dataOf1 = Maps.newHashMap();
    dataOf1.put(1, "one");
    dataOf1.put(2, "two");
    dataOf1.put(3, "three");

    Map<Integer, String> dataOf2 = Maps.newHashMap();
    dataOf2.put(4, "four");
    dataOf2.put(5, "five");
    dataOf2.put(6, "six");

    pair1.second.save(dataOf1, 1);
    pair2.second.save(dataOf2, 2);

    pair1.second.setup(pair1.first);
    Assert.assertEquals("largest recovery window", 1, pair1.second.getLargestCompletedWindow());

    pair2.second.setup(pair2.first);
    Assert.assertEquals("largest recovery window", 2, pair2.second.getLargestCompletedWindow());

    pair1.second.teardown();
    pair2.second.teardown();

    WindowDataManager manager = pair1.second.partition(1, Sets.newHashSet(2)).get(0);
    manager.setup(pair1.first);
    Assert.assertEquals("largest recovery window", 1, manager.getLargestCompletedWindow());
    manager.teardown();
  }

  @Test
  public void testDelete() throws IOException
  {
    Pair<Context.OperatorContext, FSWindowDataManager> pair1 = createManagerAndContextFor(1);
    pair1.second.getWal().setMaxLength(2);
    pair1.second.setup(pair1.first);

    Map<Integer, String> dataOf1 = Maps.newHashMap();
    dataOf1.put(1, "one");
    dataOf1.put(2, "two");
    dataOf1.put(3, "three");

    for (int i = 1; i <= 9; ++i) {
      pair1.second.save(dataOf1, i);
    }

    pair1.second.committed(3);
    pair1.second.teardown();

    Pair<Context.OperatorContext, FSWindowDataManager> pair1AfterRecovery = createManagerAndContextFor(1);
    testMeta.attributes.put(Context.OperatorContext.ACTIVATION_WINDOW_ID, 1L);
    pair1AfterRecovery.second.setup(pair1AfterRecovery.first);

    Assert.assertEquals("window 1 deleted", null, pair1AfterRecovery.second.retrieve(1));
    Assert.assertEquals("window 3 deleted", null, pair1AfterRecovery.second.retrieve(3));

    Assert.assertEquals("window 4 exists", dataOf1, pair1AfterRecovery.second.retrieve(4));
    pair1.second.teardown();
  }

  @Test
  public void testDeleteDoesNotRemoveTmpFiles() throws IOException
  {
    Pair<Context.OperatorContext, FSWindowDataManager> pair1 = createManagerAndContextFor(1);
    pair1.second.setup(pair1.first);

    Pair<Context.OperatorContext, FSWindowDataManager> pair2 = createManagerAndContextFor(2);
    pair2.second.setup(pair2.first);

    Pair<Context.OperatorContext, FSWindowDataManager> pair3 = createManagerAndContextFor(3);
    pair3.second.setup(pair3.first);

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

    for (int i = 1; i <= 9; ++i) {
      pair1.second.save(dataOf1, i);
    }

    for (int i = 1; i <= 6; ++i) {
      pair2.second.save(dataOf2, i);
    }

    for (int i = 1; i <= 3; ++i) {
      pair3.second.save(dataOf3, i);
    }

    pair1.second.teardown();
    pair2.second.teardown();
    pair3.second.teardown();

    FSWindowDataManager fsManager = (FSWindowDataManager)pair1.second.partition(1, Sets.newHashSet(2, 3)).get(0);
    fsManager.setup(pair1.first);

    Assert.assertEquals("recovery window", 3, fsManager.getLargestCompletedWindow());

    Map<Integer, Object> artifacts = fsManager.retrieveAllPartitions(1);
    Assert.assertEquals("num artifacts", 3, artifacts.size());

    fsManager.committed(3);
    fsManager.teardown();

    testMeta.attributes.put(Context.OperatorContext.ACTIVATION_WINDOW_ID, 3L);
    fsManager.setup(pair1.first);
    Assert.assertEquals("recovery window", Stateless.WINDOW_ID, fsManager.getLargestCompletedWindow());
    fsManager.teardown();
  }

  @Test
  public void testAbsoluteRecoveryPath() throws IOException
  {
    Pair<Context.OperatorContext, FSWindowDataManager> pair = createManagerAndContextFor(1);
    pair.second.setStatePathRelativeToAppPath(false);
    long time = System.currentTimeMillis();
    pair.second.setStatePath("target/" + time);

    pair.second.setup(pair.first);
    Map<Integer, String> data = Maps.newHashMap();
    data.put(1, "one");
    data.put(2, "two");
    data.put(3, "three");
    pair.second.save(data, 1);

    File recoveryDir = new File("target/" + time);
    Assert.assertTrue("recover filePath exist", recoveryDir.isDirectory());
    pair.second.teardown();
  }

  private Pair<Context.OperatorContext, FSWindowDataManager> createManagerAndContextFor(int operatorId)
  {
    FSWindowDataManager dataManager = new FSWindowDataManager();
    OperatorContext context =  mockOperatorContext(operatorId, testMeta.attributes);

    return new Pair<>(context, dataManager);
  }

}
