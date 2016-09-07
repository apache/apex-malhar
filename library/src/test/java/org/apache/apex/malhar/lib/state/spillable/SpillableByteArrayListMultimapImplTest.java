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

import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeIntSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;

import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.util.KryoCloneUtils;
import com.datatorrent.netlet.util.Slice;

public class SpillableByteArrayListMultimapImplTest
{
  public static final byte[] ID1 = new byte[]{(byte)0};

  @Rule
  public SpillableTestUtils.TestMeta testMeta = new SpillableTestUtils.TestMeta();

  @Test
  public void simpleMultiKeyTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    simpleMultiKeyTestHelper(store);
  }

  @Test
  public void simpleMultiKeyManagedStateTest()
  {
    simpleMultiKeyTestHelper(testMeta.store);
  }

  public void simpleMultiKeyTestHelper(SpillableStateStore store)
  {
    SpillableByteArrayListMultimapImpl<String, String> map =
        new SpillableByteArrayListMultimapImpl<String, String>(store, ID1, 0L, new SerdeStringSlice(),
        new SerdeStringSlice());

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long nextWindowId = 0L;
    nextWindowId = simpleMultiKeyTestHelper(store, map, "a", nextWindowId);
    nextWindowId++;

    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    Assert.assertEquals(1, map.size());

    map.endWindow();
    store.endWindow();

    nextWindowId++;
    nextWindowId = simpleMultiKeyTestHelper(store, map, "b", nextWindowId);
    nextWindowId++;

    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    Assert.assertEquals(2, map.size());

    map.endWindow();
    store.endWindow();

    nextWindowId++;
    simpleMultiKeyTestHelper(store, map, "c", nextWindowId);

    nextWindowId++;
    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    Assert.assertEquals(3, map.size());

    map.endWindow();
    store.endWindow();

    map.teardown();
    store.teardown();
  }

  public long simpleMultiKeyTestHelper(SpillableStateStore store,
      SpillableByteArrayListMultimapImpl<String, String> map, String key, long nextWindowId)
  {
    SerdeStringSlice serdeString = new SerdeStringSlice();
    SerdeIntSlice serdeInt = new SerdeIntSlice();

    Slice keySlice = serdeString.serialize(key);

    byte[] keyBytes = SliceUtils.concatenate(ID1, keySlice.toByteArray());

    nextWindowId++;
    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    Assert.assertNull(map.get(key));

    Assert.assertFalse(map.containsKey(key));

    map.put(key, "a");

    Assert.assertTrue(map.containsKey(key));

    List<String> list1 = map.get(key);
    Assert.assertEquals(1, list1.size());

    Assert.assertEquals("a", list1.get(0));

    list1.addAll(Lists.newArrayList("a", "b", "c", "d", "e", "f", "g"));

    Assert.assertEquals(8, list1.size());

    Assert.assertEquals("a", list1.get(0));
    Assert.assertEquals("a", list1.get(1));
    Assert.assertEquals("b", list1.get(2));
    Assert.assertEquals("c", list1.get(3));
    Assert.assertEquals("d", list1.get(4));
    Assert.assertEquals("e", list1.get(5));
    Assert.assertEquals("f", list1.get(6));
    Assert.assertEquals("g", list1.get(7));

    map.endWindow();
    store.endWindow();

    nextWindowId++;
    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, SpillableByteArrayListMultimapImpl.SIZE_KEY_SUFFIX), 8, 0, serdeInt);

    SpillableTestUtils.checkValue(store, 0L, keyBytes, 0, Lists.<String>newArrayList("a", "a", "b", "c", "d", "e",
        "f", "g"));

    List<String> list2 = map.get(key);

    Assert.assertEquals(8, list2.size());

    Assert.assertEquals("a", list2.get(0));
    Assert.assertEquals("a", list2.get(1));
    Assert.assertEquals("b", list2.get(2));
    Assert.assertEquals("c", list2.get(3));
    Assert.assertEquals("d", list2.get(4));
    Assert.assertEquals("e", list2.get(5));
    Assert.assertEquals("f", list2.get(6));
    Assert.assertEquals("g", list2.get(7));

    list2.add("tt");
    list2.add("ab");
    list2.add("99");
    list2.add("oo");

    Assert.assertEquals("tt", list2.get(8));
    Assert.assertEquals("ab", list2.get(9));
    Assert.assertEquals("99", list2.get(10));
    Assert.assertEquals("oo", list2.get(11));

    Assert.assertEquals(12, list2.size());

    map.endWindow();
    store.endWindow();

    nextWindowId++;
    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    Assert.assertEquals(12, list2.size());

    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, SpillableByteArrayListMultimapImpl.SIZE_KEY_SUFFIX), 12, 0, serdeInt);

    SpillableTestUtils.checkValue(store, 0L, keyBytes, 0, Lists.<String>newArrayList("a", "a", "b", "c", "d", "e",
        "f", "g", "tt", "ab", "99", "oo"));

    List<String> list3 = map.get(key);

    list3.set(1, "111");
    list3.set(3, "222");
    list3.set(5, "333");
    list3.set(11, "444");

    Assert.assertEquals("a", list3.get(0));
    Assert.assertEquals("111", list3.get(1));
    Assert.assertEquals("b", list3.get(2));
    Assert.assertEquals("222", list3.get(3));
    Assert.assertEquals("d", list3.get(4));
    Assert.assertEquals("333", list3.get(5));
    Assert.assertEquals("f", list3.get(6));
    Assert.assertEquals("g", list3.get(7));
    Assert.assertEquals("tt", list3.get(8));
    Assert.assertEquals("ab", list3.get(9));
    Assert.assertEquals("99", list3.get(10));
    Assert.assertEquals("444", list3.get(11));

    Assert.assertEquals(12, list2.size());

    map.endWindow();
    store.endWindow();

    nextWindowId++;
    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, SpillableByteArrayListMultimapImpl.SIZE_KEY_SUFFIX), 12, 0, serdeInt);

    SpillableTestUtils.checkValue(store, 0L, keyBytes, 0, Lists.<String>newArrayList("a", "111", "b", "222", "d", "333",
        "f", "g", "tt", "ab", "99", "444"));

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(nextWindowId);

    return nextWindowId;
  }

  @Test
  public void recoveryTestWithManagedState()
  {
    SpillableStateStore store = testMeta.store;

    SpillableByteArrayListMultimapImpl<String, String> map =
        new SpillableByteArrayListMultimapImpl<>(store, ID1, 0L, new SerdeStringSlice(), new SerdeStringSlice());

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long nextWindowId = 0L;
    nextWindowId = simpleMultiKeyTestHelper(store, map, "a", nextWindowId);
    long activationWindow = nextWindowId;
    nextWindowId++;

    SpillableByteArrayListMultimapImpl<String, String> clonedMap = KryoCloneUtils.cloneObject(map);
    store.checkpointed(nextWindowId);
    store.committed(nextWindowId);

    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    List<String> list1 = map.get("a");

    Assert.assertEquals(12, list1.size());

    Assert.assertEquals("a", list1.get(0));
    Assert.assertEquals("111", list1.get(1));
    Assert.assertEquals("b", list1.get(2));
    Assert.assertEquals("222", list1.get(3));
    Assert.assertEquals("d", list1.get(4));
    Assert.assertEquals("333", list1.get(5));
    Assert.assertEquals("f", list1.get(6));
    Assert.assertEquals("g", list1.get(7));
    Assert.assertEquals("tt", list1.get(8));
    Assert.assertEquals("ab", list1.get(9));
    Assert.assertEquals("99", list1.get(10));
    Assert.assertEquals("444", list1.get(11));

    list1.add("111");

    Assert.assertEquals("a", list1.get(0));
    Assert.assertEquals("111", list1.get(1));
    Assert.assertEquals("b", list1.get(2));
    Assert.assertEquals("222", list1.get(3));
    Assert.assertEquals("d", list1.get(4));
    Assert.assertEquals("333", list1.get(5));
    Assert.assertEquals("f", list1.get(6));
    Assert.assertEquals("g", list1.get(7));
    Assert.assertEquals("tt", list1.get(8));
    Assert.assertEquals("ab", list1.get(9));
    Assert.assertEquals("99", list1.get(10));
    Assert.assertEquals("444", list1.get(11));
    Assert.assertEquals("111", list1.get(12));

    Assert.assertEquals(13, list1.size());

    map.endWindow();
    store.endWindow();

    map.teardown();
    store.teardown();

    map = clonedMap;
    store = map.getStore();

    Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_PATH, testMeta.applicationPath);
    attributes.put(Context.OperatorContext.ACTIVATION_WINDOW_ID, activationWindow);
    Context.OperatorContext context =
        new OperatorContextTestHelper.TestIdOperatorContext(testMeta.operatorContext.getId(), attributes);

    store.setup(context);
    map.setup(context);

    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    SerdeStringSlice serdeString = new SerdeStringSlice();
    Slice keySlice = serdeString.serialize("a");
    byte[] keyBytes = SliceUtils.concatenate(ID1, keySlice.toByteArray());

    SpillableTestUtils.checkValue(store, 0L, keyBytes, 0, Lists.<String>newArrayList("a", "111", "b", "222", "d",
        "333", "f", "g", "tt", "ab", "99", "444"));

    Assert.assertEquals(1, map.size());
    Assert.assertEquals(12, map.get("a").size());

    map.endWindow();
    store.endWindow();

    map.teardown();
    store.teardown();
  }
  
  @Test
  public void testLoad()
  {
    Random random = new Random();
    final int keySize = 1000000;
    final int valueSize = 100000000;
    final int numOfEntry = 100000;

    SpillableStateStore store = testMeta.store;
    
    SpillableByteArrayListMultimapImpl<String, String> multimap = new SpillableByteArrayListMultimapImpl<>(
        this.testMeta.store, ID1, 0L, new SerdeStringSlice(), new SerdeStringSlice());
    
    Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_PATH, testMeta.applicationPath);
    Context.OperatorContext context =
        new OperatorContextTestHelper.TestIdOperatorContext(testMeta.operatorContext.getId(), attributes);
    store.setup(context);
    multimap.setup(context);

    store.beginWindow(1);
    multimap.beginWindow(1);
    for (int i = 0; i < numOfEntry; ++i) {
      multimap.put(String.valueOf(random.nextInt(keySize)), String.valueOf(random.nextInt(valueSize)));
    }
    multimap.endWindow();
    store.endWindow();
  }

  @Test
  public void testRemoveAll()
  {
    SpillableStateStore store = testMeta.store;
    SpillableByteArrayListMultimapImpl<String, String> map =
        new SpillableByteArrayListMultimapImpl<String, String>(store, ID1, 0L, new SerdeStringSlice(),
            new SerdeStringSlice());

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long windowId = 0L;

    store.beginWindow(windowId);
    map.beginWindow(windowId);
    map.put("key1", "a1");
    map.put("key1", "a2");
    map.put("key2", "b");
    map.put("key3", "c");
    Assert.assertTrue(map.containsKey("key1"));
    Assert.assertTrue(map.containsKey("key2"));
    Assert.assertTrue(map.containsKey("key3"));

    Assert.assertTrue(map.containsEntry("key1", "a1"));
    Assert.assertTrue(map.containsEntry("key1", "a2"));
    Assert.assertFalse(map.containsEntry("key1", "a3"));
    Assert.assertTrue(map.containsEntry("key2", "b"));
    Assert.assertTrue(map.containsEntry("key3", "c"));

    map.endWindow();
    store.endWindow();
    windowId++;

    store.beginWindow(windowId);
    map.beginWindow(windowId);
    map.removeAll("key1");
    map.removeAll("key2");
    Assert.assertFalse(map.containsKey("key1"));
    Assert.assertFalse(map.containsKey("key2"));
    Assert.assertTrue(map.containsKey("key3"));
    map.endWindow();
    store.endWindow();
    windowId++;

    store.beginWindow(windowId);
    map.beginWindow(windowId);
    map.put("key1", "a3");
    Assert.assertTrue(map.containsKey("key1"));
    Assert.assertFalse(map.containsKey("key2"));
    Assert.assertTrue(map.containsKey("key3"));
    List<String> values = map.get("key1");
    Assert.assertEquals(1, values.size());
    Assert.assertEquals("a3", values.get(0));
    int i = 0;
    for (String val : values) {
      i++;
    }
    Assert.assertEquals(1, i);
    map.endWindow();
    store.endWindow();
    windowId++;

    Assert.assertEquals(1, map.size());

    map.endWindow();
    store.endWindow();

  }
}
