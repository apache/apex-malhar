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

import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.StringSerde;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

import com.datatorrent.api.DAG;

public class SpillableSetMultimapImplTest
{
  public static final byte[] ID1 = new byte[]{(byte)0};

  @Rule
  public SpillableTestUtils.TestMeta testMeta = new SpillableTestUtils.TestMeta();

  public TimeExtractor<String> te = null;

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

  @Test
  public void simpleMultiKeyTimeUnifiedManagedStateTest()
  {
    te = new TestStringTimeExtractor();
    simpleMultiKeyTestHelper(testMeta.timeStore);
  }


  public void simpleMultiKeyTestHelper(SpillableStateStore store)
  {
    SpillableSetMultimapImpl<String, String> map = null;
    if (te == null) {
      map = new SpillableSetMultimapImpl<>(store, ID1, 0L, createStringSerde(), createStringSerde());
    } else {
      map = new SpillableSetMultimapImpl<>(store, ID1, 0L, createStringSerde(), createStringSerde(), te);
    }

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
      SpillableSetMultimapImpl<String, String> map, String key, long nextWindowId)
  {
    nextWindowId++;
    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    Assert.assertNull(map.get(key));

    Assert.assertFalse(map.containsKey(key));

    map.put(key, "a");

    Assert.assertTrue(map.containsKey(key));

    Set<String> set1 = map.get(key);
    Assert.assertEquals(1, set1.size());
    Iterator<String> it = set1.iterator();

    Assert.assertEquals("a", it.next());

    map.endWindow();
    store.endWindow();

    nextWindowId++;
    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    map.removeAll(key);
    Assert.assertFalse(map.containsKey(key));

    map.endWindow();
    store.endWindow();

    nextWindowId++;
    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    Assert.assertFalse(map.containsKey(key));
    map.put(key, "a");
    set1 = map.get(key);
    Assert.assertEquals(1, set1.size());
    set1.addAll(Lists.newArrayList("a", "b", "c", "d", "e", "f", "g"));
    Assert.assertEquals(7, set1.size());

    Set<String> referenceSet = Sets.newHashSet("a", "b", "c", "d", "e", "f", "g");
    Assert.assertTrue(referenceSet.containsAll(set1));
    Assert.assertTrue(set1.containsAll(referenceSet));

    map.endWindow();
    store.endWindow();

    nextWindowId++;
    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    Set<String> set2 = map.get(key);

    Assert.assertEquals(7, set2.size());
    Assert.assertTrue(referenceSet.containsAll(set2));
    Assert.assertTrue(set2.containsAll(referenceSet));

    set2.add("tt");
    set2.add("ab");
    set2.add("99");
    set2.add("oo");
    referenceSet = Sets.newHashSet("a", "b", "c", "d", "e", "f", "g", "tt", "ab", "99", "oo");
    Assert.assertTrue(referenceSet.containsAll(set2));
    Assert.assertTrue(set2.containsAll(referenceSet));

    Assert.assertEquals(11, set2.size());

    map.endWindow();
    store.endWindow();

    nextWindowId++;
    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    Assert.assertEquals(11, set2.size());

    map.endWindow();
    store.endWindow();

    return nextWindowId;
  }

  @Test
  public void recoveryTestWithManagedState()
  {
    SpillableStateStore store = testMeta.store;

    SpillableSetMultimapImpl<String, String> map =
        new SpillableSetMultimapImpl<>(store, ID1, 0L, createStringSerde(), createStringSerde());

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long nextWindowId = 0L;
    nextWindowId = simpleMultiKeyTestHelper(store, map, "a", nextWindowId);
    long activationWindow = nextWindowId;
    store.beforeCheckpoint(nextWindowId);
    SpillableSetMultimapImpl<String, String> clonedMap = KryoCloneUtils.cloneObject(map);
    store.checkpointed(nextWindowId);
    store.committed(nextWindowId);

    nextWindowId++;

    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    Set<String> set1 = map.get("a");

    Assert.assertEquals(11, set1.size());

    Set<String> referenceSet = Sets.newHashSet("a", "b", "c", "d", "e", "f", "g", "tt", "ab", "99", "oo");
    Assert.assertTrue(referenceSet.containsAll(set1));
    Assert.assertTrue(set1.containsAll(referenceSet));

    set1.add("111");

    Assert.assertTrue(set1.contains("111"));

    Assert.assertEquals(12, set1.size());

    map.endWindow();
    store.endWindow();

    map.teardown();
    store.teardown();

    map = clonedMap;
    store = map.getStore();

    Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_PATH, testMeta.applicationPath);
    attributes.put(Context.OperatorContext.ACTIVATION_WINDOW_ID, activationWindow);
    OperatorContext context = mockOperatorContext(testMeta.operatorContext.getId(), attributes);

    store.setup(context);
    map.setup(context);

    nextWindowId = activationWindow + 1;

    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    Assert.assertEquals(1, map.size());
    Assert.assertTrue(map.containsKey("a"));
    Assert.assertEquals(11, map.get("a").size());

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

    SpillableSetMultimapImpl<String, String> multimap = new SpillableSetMultimapImpl<>(testMeta.store, ID1, 0L,
        createStringSerde(), createStringSerde());

    Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_PATH, testMeta.applicationPath);
    OperatorContext context = mockOperatorContext(testMeta.operatorContext.getId(), attributes);
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

  private Serde<String> createStringSerde()
  {
    return new StringSerde();
  }
}
