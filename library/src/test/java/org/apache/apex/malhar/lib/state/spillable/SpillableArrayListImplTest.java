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
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.apex.malhar.lib.utils.serde.StringSerde;

import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

import com.datatorrent.api.DAG;

public class SpillableArrayListImplTest
{
  public static final byte[] ID1 = new byte[]{(byte)0};
  public static final byte[] ID2 = new byte[]{(byte)1};

  @Rule
  public SpillableTestUtils.TestMeta testMeta = new SpillableTestUtils.TestMeta();

  @Test
  public void simpleAddGetAndSetTest1()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    simpleAddGetAndSetTest1Helper(store);
  }

  @Test
  public void simpleAddGetAndSetManagedStateTest1()
  {
    simpleAddGetAndSetTest1Helper(testMeta.store);
  }

  public void simpleAddGetAndSetTest1Helper(SpillableStateStore store)
  {
    SpillableArrayListImpl<String> list = new SpillableArrayListImpl<>(0L, ID1, store,
        new StringSerde(), 1);

    store.setup(testMeta.operatorContext);
    list.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    SpillableTestUtils.checkOutOfBounds(list, 0);
    Assert.assertEquals(0, list.size());

    list.add("a");

    SpillableTestUtils.checkOutOfBounds(list, 1);
    Assert.assertEquals(1, list.size());

    Assert.assertEquals("a", list.get(0));

    list.addAll(Lists.newArrayList("a", "b", "c"));

    Assert.assertEquals(4, list.size());

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("a", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("c", list.get(3));

    SpillableTestUtils.checkOutOfBounds(list, 4);

    list.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.newArrayList("a"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.newArrayList("a"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.newArrayList("b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.newArrayList("c"));

    Assert.assertEquals(4, list.size());

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("a", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("c", list.get(3));

    list.add("tt");
    list.add("ab");
    list.add("99");
    list.add("oo");

    Assert.assertEquals("tt", list.get(4));
    Assert.assertEquals("ab", list.get(5));
    Assert.assertEquals("99", list.get(6));
    Assert.assertEquals("oo", list.get(7));

    list.set(1, "111");

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("111", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("c", list.get(3));
    Assert.assertEquals("tt", list.get(4));
    Assert.assertEquals("ab", list.get(5));
    Assert.assertEquals("99", list.get(6));
    Assert.assertEquals("oo", list.get(7));

    list.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.newArrayList("a"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.newArrayList("111"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.newArrayList("b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.newArrayList("c"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 4, Lists.newArrayList("tt"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 5, Lists.newArrayList("ab"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 6, Lists.newArrayList("99"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 7, Lists.newArrayList("oo"));

    list.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    list.teardown();
    store.teardown();
  }

  @Test
  public void simpleAddGetAndSetTest3()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    simpleAddGetAndSetTest3Helper(store);
  }

  @Test
  public void simpleAddGetAndSetManagedStateTest3()
  {
    simpleAddGetAndSetTest3Helper(testMeta.store);
  }

  private void simpleAddGetAndSetTest3Helper(SpillableStateStore store)
  {
    SpillableArrayListImpl<String> list = new SpillableArrayListImpl<>(0L, ID1, store,
        new StringSerde(), 3);

    store.setup(testMeta.operatorContext);
    list.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    SpillableTestUtils.checkOutOfBounds(list, 0);
    Assert.assertEquals(0, list.size());

    list.add("a");

    SpillableTestUtils.checkOutOfBounds(list, 1);
    Assert.assertEquals(1, list.size());

    Assert.assertEquals("a", list.get(0));

    list.addAll(Lists.newArrayList("a", "b", "c", "d", "e", "f", "g"));

    Assert.assertEquals(8, list.size());

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("a", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("c", list.get(3));
    Assert.assertEquals("d", list.get(4));
    Assert.assertEquals("e", list.get(5));
    Assert.assertEquals("f", list.get(6));
    Assert.assertEquals("g", list.get(7));

    SpillableTestUtils.checkOutOfBounds(list, 20);

    list.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.newArrayList("a", "a", "b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.newArrayList("c", "d", "e"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.newArrayList("f", "g"));

    Assert.assertEquals(8, list.size());

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("a", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("c", list.get(3));
    Assert.assertEquals("d", list.get(4));
    Assert.assertEquals("e", list.get(5));
    Assert.assertEquals("f", list.get(6));
    Assert.assertEquals("g", list.get(7));

    list.add("tt");
    list.add("ab");
    list.add("99");
    list.add("oo");

    Assert.assertEquals("tt", list.get(8));
    Assert.assertEquals("ab", list.get(9));
    Assert.assertEquals("99", list.get(10));
    Assert.assertEquals("oo", list.get(11));

    list.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.newArrayList("a", "a", "b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.newArrayList("c", "d", "e"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.newArrayList("f", "g", "tt"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.newArrayList("ab", "99", "oo"));

    list.set(1, "111");
    list.set(3, "222");
    list.set(5, "333");
    list.set(11, "444");

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("111", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("222", list.get(3));
    Assert.assertEquals("d", list.get(4));
    Assert.assertEquals("333", list.get(5));
    Assert.assertEquals("f", list.get(6));
    Assert.assertEquals("g", list.get(7));
    Assert.assertEquals("tt", list.get(8));
    Assert.assertEquals("ab", list.get(9));
    Assert.assertEquals("99", list.get(10));
    Assert.assertEquals("444", list.get(11));

    list.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.newArrayList("a", "111", "b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.newArrayList("222", "d", "333"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.newArrayList("f", "g", "tt"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.newArrayList("ab", "99", "444"));

    list.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    list.teardown();
    store.teardown();
  }

  @Test
  public void simpleMultiListTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    simpleMultiListTestHelper(store);
  }

  @Test
  public void simpleMultiListManagedStateTest()
  {
    simpleMultiListTestHelper(testMeta.store);
  }

  public void simpleMultiListTestHelper(SpillableStateStore store)
  {
    SpillableArrayListImpl<String> list1 = new SpillableArrayListImpl<>(0L, ID1, store,
        new StringSerde(), 1);

    SpillableArrayListImpl<String> list2 = new SpillableArrayListImpl<>(0L, ID2, store,
        new StringSerde(), 1);

    store.setup(testMeta.operatorContext);
    list1.setup(testMeta.operatorContext);
    list2.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    list1.beginWindow(windowId);
    list2.beginWindow(windowId);

    SpillableTestUtils.checkOutOfBounds(list1, 0);
    Assert.assertEquals(0, list1.size());

    list1.add("a");

    SpillableTestUtils.checkOutOfBounds(list2, 0);

    list2.add("2a");

    SpillableTestUtils.checkOutOfBounds(list1, 1);
    SpillableTestUtils.checkOutOfBounds(list2, 1);

    Assert.assertEquals(1, list1.size());
    Assert.assertEquals(1, list2.size());

    Assert.assertEquals("a", list1.get(0));
    Assert.assertEquals("2a", list2.get(0));

    list1.addAll(Lists.newArrayList("a", "b", "c"));
    list2.addAll(Lists.newArrayList("2a", "2b"));

    Assert.assertEquals(4, list1.size());
    Assert.assertEquals(3, list2.size());

    Assert.assertEquals("a", list1.get(0));
    Assert.assertEquals("a", list1.get(1));
    Assert.assertEquals("b", list1.get(2));
    Assert.assertEquals("c", list1.get(3));

    Assert.assertEquals("2a", list2.get(0));
    Assert.assertEquals("2a", list2.get(1));
    Assert.assertEquals("2b", list2.get(2));

    SpillableTestUtils.checkOutOfBounds(list1, 4);
    SpillableTestUtils.checkOutOfBounds(list2, 3);

    list1.endWindow();
    list2.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.newArrayList("a"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.newArrayList("a"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.newArrayList("b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.newArrayList("c"));

    SpillableTestUtils.checkValue(store, 0L, ID2, 0, Lists.newArrayList("2a"));
    SpillableTestUtils.checkValue(store, 0L, ID2, 1, Lists.newArrayList("2a"));
    SpillableTestUtils.checkValue(store, 0L, ID2, 2, Lists.newArrayList("2b"));

    windowId++;
    store.beginWindow(windowId);
    list1.beginWindow(windowId);
    list2.beginWindow(windowId);

    Assert.assertEquals(4, list1.size());
    Assert.assertEquals(3, list2.size());

    Assert.assertEquals("a", list1.get(0));
    Assert.assertEquals("a", list1.get(1));
    Assert.assertEquals("b", list1.get(2));
    Assert.assertEquals("c", list1.get(3));

    Assert.assertEquals("2a", list2.get(0));
    Assert.assertEquals("2a", list2.get(1));
    Assert.assertEquals("2b", list2.get(2));

    list1.add("tt");
    list1.add("ab");
    list1.add("99");
    list1.add("oo");

    list2.add("2tt");
    list2.add("2ab");

    Assert.assertEquals("tt", list1.get(4));
    Assert.assertEquals("ab", list1.get(5));
    Assert.assertEquals("99", list1.get(6));
    Assert.assertEquals("oo", list1.get(7));

    Assert.assertEquals("2tt", list2.get(3));
    Assert.assertEquals("2ab", list2.get(4));

    list1.set(1, "111");
    list2.set(1, "2111");

    Assert.assertEquals("a", list1.get(0));
    Assert.assertEquals("111", list1.get(1));
    Assert.assertEquals("b", list1.get(2));
    Assert.assertEquals("c", list1.get(3));
    Assert.assertEquals("tt", list1.get(4));
    Assert.assertEquals("ab", list1.get(5));
    Assert.assertEquals("99", list1.get(6));
    Assert.assertEquals("oo", list1.get(7));

    Assert.assertEquals("2a", list2.get(0));
    Assert.assertEquals("2111", list2.get(1));
    Assert.assertEquals("2b", list2.get(2));
    Assert.assertEquals("2tt", list2.get(3));
    Assert.assertEquals("2ab", list2.get(4));

    list1.endWindow();
    list2.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    list1.beginWindow(windowId);
    list2.beginWindow(windowId);

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.newArrayList("a"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.newArrayList("111"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.newArrayList("b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.newArrayList("c"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 4, Lists.newArrayList("tt"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 5, Lists.newArrayList("ab"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 6, Lists.newArrayList("99"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 7, Lists.newArrayList("oo"));

    SpillableTestUtils.checkValue(store, 0L, ID2, 0, Lists.newArrayList("2a"));
    SpillableTestUtils.checkValue(store, 0L, ID2, 1, Lists.newArrayList("2111"));
    SpillableTestUtils.checkValue(store, 0L, ID2, 2, Lists.newArrayList("2b"));
    SpillableTestUtils.checkValue(store, 0L, ID2, 3, Lists.newArrayList("2tt"));
    SpillableTestUtils.checkValue(store, 0L, ID2, 4, Lists.newArrayList("2ab"));

    list1.endWindow();
    list2.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    list1.teardown();
    list2.teardown();
    store.teardown();
  }

  @Test
  public void recoveryManagedStateTest()
  {
    SpillableStateStore store = testMeta.store;

    SpillableArrayListImpl<String> list = new SpillableArrayListImpl<>(0L, ID1, store,
        new StringSerde(), 3);

    store.setup(testMeta.operatorContext);
    list.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    SpillableTestUtils.checkOutOfBounds(list, 0);

    list.add("a");
    list.addAll(Lists.newArrayList("a", "b", "c", "d", "e", "f", "g"));

    Assert.assertEquals(8, list.size());

    list.endWindow();
    store.endWindow();

    windowId++;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    list.add("tt");
    list.add("ab");
    list.add("99");
    list.add("oo");

    list.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    list.set(1, "111");
    list.set(3, "222");
    list.set(5, "333");
    list.set(11, "444");

    list.endWindow();
    store.endWindow();

    windowId++;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    list.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    long activationWindow = windowId;
    SpillableArrayListImpl<String> clonedList = KryoCloneUtils.cloneObject(list);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    list.set(1, "111111");
    list.set(3, "222222");
    list.add("xyz");

    list.endWindow();
    store.endWindow();

    list.teardown();
    store.teardown();

    Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_PATH, testMeta.applicationPath);
    attributes.put(Context.OperatorContext.ACTIVATION_WINDOW_ID, activationWindow);
    OperatorContext context = mockOperatorContext(testMeta.operatorContext.getId(), attributes);

    list = clonedList;
    store = clonedList.getStore();

    store.setup(context);
    list.setup(context);

    windowId = activationWindow + 1L;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("111", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("222", list.get(3));
    Assert.assertEquals("d", list.get(4));
    Assert.assertEquals("333", list.get(5));
    Assert.assertEquals("f", list.get(6));
    Assert.assertEquals("g", list.get(7));
    Assert.assertEquals("tt", list.get(8));
    Assert.assertEquals("ab", list.get(9));
    Assert.assertEquals("99", list.get(10));
    Assert.assertEquals("444", list.get(11));
    Assert.assertEquals(12, list.size());

    list.endWindow();
    store.endWindow();

    list.teardown();
    store.teardown();
  }
}
