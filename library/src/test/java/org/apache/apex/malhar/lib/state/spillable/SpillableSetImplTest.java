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

import java.util.HashSet;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.StringSerde;

import com.google.common.collect.Lists;

public class SpillableSetImplTest
{
  public static final byte[] ID1 = new byte[]{(byte)0};

  @Rule
  public SpillableTestUtils.TestMeta testMeta = new SpillableTestUtils.TestMeta();

  public TimeExtractor<String> te = null;

  @Test
  public void simpleAddGetAndSetTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    simpleAddGetAndSetTestHelper(store);
  }

  @Test
  public void simpleAddGetAndSetTimeUnifiedManagedStateTest()
  {
    te = new TestStringTimeExtractor();
    simpleAddGetAndSetTestHelper(testMeta.timeStore);
  }

  @Test
  public void simpleAddGetAndSetManagedStateTest()
  {
    simpleAddGetAndSetTestHelper(testMeta.store);
  }

  public void simpleAddGetAndSetTestHelper(SpillableStateStore store)
  {
    SpillableSetImpl<String> set;

    if (te == null) {
      set = new SpillableSetImpl<>(0L, ID1, store, new StringSerde());
    } else {
      set = new SpillableSetImpl<>(ID1, store, new StringSerde(), te);
    }
    store.setup(testMeta.operatorContext);
    set.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    set.beginWindow(windowId);

    Assert.assertEquals(0, set.size());

    set.add("a");

    Assert.assertEquals(1, set.size());

    Assert.assertTrue(set.contains("a"));

    set.addAll(Lists.newArrayList("a", "b", "c"));

    Assert.assertEquals(3, set.size());

    Assert.assertTrue(set.contains("a"));
    Assert.assertTrue(set.contains("b"));
    Assert.assertTrue(set.contains("c"));

    HashSet<String> result = new HashSet<>();
    Iterator<String> it = set.iterator();
    int i = 0;
    while (it.hasNext()) {
      result.add(it.next());
      i++;
    }
    Assert.assertTrue(result.containsAll(Lists.newArrayList("a", "b", "c")));
    Assert.assertEquals(3, i);

    it = set.iterator();
    while (it.hasNext()) {
      if ("b".equals(it.next())) {
        it.remove();
      }
    }
    Assert.assertEquals(2, set.size());
    Assert.assertTrue(set.contains("a"));
    Assert.assertFalse(set.contains("b"));
    Assert.assertTrue(set.contains("c"));

    set.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    set.beginWindow(windowId);

    set.add("tt");
    set.add("ab");
    set.add("99");
    set.add("oo");

    Assert.assertTrue(set.contains("tt"));
    Assert.assertTrue(set.contains("ab"));
    Assert.assertTrue(set.contains("99"));
    Assert.assertTrue(set.contains("oo"));

    set.remove("ab");

    Assert.assertTrue(set.contains("tt"));
    Assert.assertFalse(set.contains("ab"));
    Assert.assertTrue(set.contains("99"));
    Assert.assertTrue(set.contains("oo"));

    set.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    set.beginWindow(windowId);

    set.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    set.teardown();
    store.teardown();
  }
}
