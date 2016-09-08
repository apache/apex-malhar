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
package org.apache.apex.malhar.lib.state.spillable;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeIntSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.util.KryoCloneUtils;

public class SpillableTwoKeyByteMapImplTest
{
  public static final byte[] ID1 = new byte[]{(byte)0};
  public static final byte[] ID2 = new byte[]{(byte)1};

  @Rule
  public SpillableTestUtils.TestMeta testMeta = new SpillableTestUtils.TestMeta();

  @Test
  public void simpleGetAndPutTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    simpleGetAndPutTestHelper(store);
  }

  @Test
  public void simpleGetAndPutManagedStateTest()
  {
    simpleGetAndPutTestHelper(testMeta.store);
  }

  private void simpleGetAndPutTestHelper(SpillableStateStore store)
  {
    SerdeStringSlice sss = new SerdeStringSlice();

    SpillableTwoKeyByteMapImpl<String, String, Integer> map = new SpillableTwoKeyByteMapImpl<>(store, ID1, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice(),
        new SerdeIntSlice());

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    Assert.assertEquals(0, map.size());

    map.put("I", "a", 1);
    map.put("II", "b", 2);
    map.put("II", "c", 3);

    Assert.assertEquals(3, map.size());

    Assert.assertEquals(1, map.get("I", "a").intValue());
    Assert.assertEquals(2, map.get("II", "b").intValue());
    Assert.assertEquals(3, map.get("II", "c").intValue());
    Assert.assertEquals(null, map.get("II", "d"));
    Assert.assertEquals(null, map.get("III", "d"));

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    Assert.assertEquals(3, map.size());

    Assert.assertEquals(1, map.get("I", "a").intValue());
    Assert.assertEquals(2, map.get("II", "b").intValue());
    Assert.assertEquals(3, map.get("II", "c").intValue());
    Assert.assertEquals(null, map.get("III", "d"));

    map.put("III", "d", 4);
    map.put("III", "e", 5);
    map.put("III", "f", 6);

    Assert.assertEquals(6, map.size());

    Assert.assertEquals(4, map.get("III", "d").intValue());
    Assert.assertEquals(5, map.get("III", "e").intValue());
    Assert.assertEquals(6, map.get("III", "f").intValue());

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    map.teardown();
    store.teardown();
  }

  @Test
  public void simpleRemoveTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    simpleRemoveTestHelper(store);
  }

  @Test
  public void simpleRemoveManagedStateTest()
  {
    simpleRemoveTestHelper(testMeta.store);
  }

  private void simpleRemoveTestHelper(SpillableStateStore store)
  {
    SpillableTwoKeyByteMapImpl<String, String, Integer> map = new SpillableTwoKeyByteMapImpl<>(store, ID1, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice(),
        new SerdeIntSlice());

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    Assert.assertEquals(0, map.size());

    map.put("I", "a", 1);
    map.put("II", "b", 2);
    map.put("II", "c", 3);

    Assert.assertEquals(3, map.size());

    map.remove("II");

    Assert.assertEquals(1, map.get("I", "a").intValue());
    Assert.assertEquals(null, map.get("II", "b"));
    Assert.assertEquals(null, map.get("II", "c"));
    Assert.assertEquals(null, map.get("III", "d"));

    Assert.assertEquals(1, map.size());

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    Assert.assertEquals(1, map.size());

    Assert.assertEquals(1, map.get("I", "a").intValue());
    Assert.assertEquals(null, map.get("II", "b"));
    Assert.assertEquals(null, map.get("II", "c"));
    Assert.assertEquals(null, map.get("III", "d"));

    map.put("III", "d", 4);
    map.put("III", "e", 5);
    map.put("III", "f", 6);

    Assert.assertEquals(4, map.size());

    Assert.assertEquals(4, map.get("III", "d").intValue());
    Assert.assertEquals(5, map.get("III", "e").intValue());
    Assert.assertEquals(6, map.get("III", "f").intValue());

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    map.remove("I", "a");
    map.remove("III", "d");
    Assert.assertEquals(null, map.get("I", "a"));
    Assert.assertEquals(null, map.get("II", "b"));
    Assert.assertEquals(null, map.get("II", "c"));
    Assert.assertEquals(null, map.get("III", "d"));
    Assert.assertEquals(5, map.get("III", "e").intValue());
    Assert.assertEquals(6, map.get("III", "f").intValue());
    Assert.assertEquals(null, map.get("III", "g"));

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    map.teardown();
    store.teardown();
  }

  @Test
  public void multiMapPerBucketTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    multiMapPerBucketTestHelper(store);
  }

  @Test
  public void multiMapPerBucketManagedStateTest()
  {
    multiMapPerBucketTestHelper(testMeta.store);
  }

  public void multiMapPerBucketTestHelper(SpillableStateStore store)
  {
    SpillableTwoKeyByteMapImpl<String, String, Integer> map1 = new SpillableTwoKeyByteMapImpl<>(store, ID1, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice(),
        new SerdeIntSlice());
    SpillableTwoKeyByteMapImpl<String, String, Integer> map2 = new SpillableTwoKeyByteMapImpl<>(store, ID2, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice(),
        new SerdeIntSlice());

    store.setup(testMeta.operatorContext);
    map1.setup(testMeta.operatorContext);
    map2.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    map1.beginWindow(windowId);
    map2.beginWindow(windowId);

    map1.put("I", "a", 1);

    Assert.assertEquals(1, map1.get("I", "a").intValue());
    Assert.assertEquals(null, map2.get("I", "a"));

    map2.put("I", "a", 5);

    Assert.assertEquals(1, map1.get("I", "a").intValue());
    Assert.assertEquals(5, map2.get("I", "a").intValue());

    map1.put("II", "b", 2);
    map2.put("II", "c", 3);

    Assert.assertEquals(1, map1.get("I", "a").intValue());
    Assert.assertEquals(2, map1.get("II", "b").intValue());

    Assert.assertEquals(5, map2.get("I", "a").intValue());
    Assert.assertEquals(null, map2.get("II", "b"));
    Assert.assertEquals(3, map2.get("II", "c").intValue());

    map1.endWindow();
    map2.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map1.beginWindow(windowId);
    map2.beginWindow(windowId);

    map1.remove("I", "a");

    Assert.assertEquals(null, map1.get("I", "a"));
    Assert.assertEquals(5, map2.get("I", "a").intValue());

    map1.endWindow();
    map2.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map1.beginWindow(windowId);
    map2.beginWindow(windowId);

    map1.endWindow();
    map2.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);

    map1.teardown();
    map2.teardown();
    store.teardown();
  }

  @Test
  public void recoveryWithManagedStateTest() throws Exception
  {
    SpillableTwoKeyByteMapImpl<String, String, Integer> map1 = new SpillableTwoKeyByteMapImpl<>(testMeta.store, ID1, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice(),
        new SerdeIntSlice());

    testMeta.store.setup(testMeta.operatorContext);
    map1.setup(testMeta.operatorContext);

    testMeta.store.beginWindow(0);
    map1.beginWindow(0);
    map1.put("I", "x", 1);
    map1.put("I", "y", 2);
    map1.put("II", "z", 3);
    map1.put("III", "zz", 33);
    Assert.assertEquals(4, map1.size());
    map1.endWindow();
    testMeta.store.endWindow();

    testMeta.store.beginWindow(1);
    map1.beginWindow(1);
    Assert.assertEquals(4, map1.size());
    map1.put("I", "x", 4);
    map1.put("I", "y", 5);
    map1.remove("III", "zz");
    Assert.assertEquals(3, map1.size());
    map1.endWindow();
    testMeta.store.endWindow();
    testMeta.store.beforeCheckpoint(1);
    testMeta.store.checkpointed(1);

    SpillableTwoKeyByteMapImpl<String, String, Integer> clonedMap1 = KryoCloneUtils.cloneObject(map1);

    testMeta.store.beginWindow(2);
    map1.beginWindow(2);
    Assert.assertEquals(3, map1.size());
    map1.put("I", "x", 6);
    map1.put("I", "y", 7);
    map1.put("III", "w", 8);
    Assert.assertEquals(4, map1.size());
    map1.endWindow();
    testMeta.store.endWindow();

    // simulating crash here
    map1.teardown();
    testMeta.store.teardown();

    Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_PATH, testMeta.applicationPath);
    attributes.put(Context.OperatorContext.ACTIVATION_WINDOW_ID, 1L);
    Context.OperatorContext context =
        new OperatorContextTestHelper.TestIdOperatorContext(testMeta.operatorContext.getId(), attributes);

    map1 = clonedMap1;
    map1.getStore().setup(context);
    map1.setup(testMeta.operatorContext);

    map1.getStore().beginWindow(2);
    map1.beginWindow(2);
    Assert.assertEquals(3, map1.size());
    Assert.assertEquals(4, map1.get("I", "x").intValue());
    Assert.assertEquals(5, map1.get("I", "y").intValue());
    Assert.assertEquals(3, map1.get("II", "z").intValue());
    map1.endWindow();
    map1.getStore().endWindow();

    map1.teardown();
  }

}
