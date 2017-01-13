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

import org.apache.apex.malhar.lib.state.managed.KeyBucketExtractor;
import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedTimeStateSpillableStore;
import org.apache.apex.malhar.lib.utils.serde.StringSerde;
import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.util.KryoCloneUtils;

public class SpillableTimeSlicedMapImplTest
{
  public static final byte[] ID1 = new byte[]{(byte)0};
  public static final byte[] ID2 = new byte[]{(byte)1};

  private SpillableTimeStateStore store;

  private TimeExtractor<String> te = null;
  private KeyBucketExtractor<String> ke = null;

  @Rule
  public SpillableTimeStateTestUtils.TestMeta testMeta = new SpillableTimeStateTestUtils.TestMeta();

  private void setup()
  {
    store = testMeta.store;
    te = new TestStringTimeExtractor();
    ke = new StringKeyBucketExtractor();
  }

  @Test
  public void simpleGetAndPutTest()
  {
    setup();
    SpillableTimeSlicedMapImpl<String, String> map = new SpillableTimeSlicedMapImpl<>(store,ID1,new StringSerde(), new StringSerde(), te, ke);
    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    Assert.assertEquals(0, map.size());

    map.put("a", "1");
    map.put("b", "2");
    map.put("c", "3");

    Assert.assertEquals(3, map.size());

    assertMultiEqualsFromMap(map, new String[]{"1", "2", "3", null}, new String[]{"a", "b", "c", "d"});

    multiValueCheck(new String[]{"a", "b", "c", "d"}, ID1, new String[]{null, null, null, null});

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    multiValueCheck(new String[]{"a", "b", "c", "d"}, ID1, new String[]{"1", "2", "3", null});

    Assert.assertEquals(3, map.size());

    assertMultiEqualsFromMap(map, new String[]{"1", "2", "3", null}, new String[]{"a", "b", "c", "d"});

    map.put("d", "4");
    map.put("e", "5");
    map.put("f", "6");

    Assert.assertEquals(6, map.size());

    assertMultiEqualsFromMap(map, new String[]{"4", "5", "6"}, new String[]{"d", "e", "f"});

    multiValueCheck(new String[]{"a", "b", "c", "d", "e", "f"}, ID1, new String[]{"1", "2", "3", null, null, null});

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);


    multiValueCheck(new String[]{"a", "b", "c", "d", "e", "f", "g"}, ID1, new String[]{"1", "2", "3", "4", "5", "6", null});

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    map.teardown();
    store.teardown();
  }

  @Test
  public void simpleGetAndPutMultiBucketTest()
  {
    setup();
    ((ManagedTimeStateSpillableStore)store).setNumBuckets(2);
    SpillableTimeSlicedMapImpl<String, String> map = new SpillableTimeSlicedMapImpl<>(store,ID1,new StringSerde(), new StringSerde(), te, ke);

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    Assert.assertEquals(0, map.size());

    map.put("a", "1");
    map.put("b", "2");
    map.put("c", "3");

    Assert.assertEquals(3, map.size());

    assertMultiEqualsFromMap(map, new String[]{"1", "2", "3", null}, new String[]{"a", "b", "c", "d"});

    multiValueCheck(new String[]{"a", "b", "c", "d"}, ID1, new String[]{null, null, null, null});

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    multiValueCheck(new String[]{"a", "b", "c", "d"}, ID1, new String[]{"1", "2", "3", null});

    Assert.assertEquals(3, map.size());

    assertMultiEqualsFromMap(map, new String[]{"1", "2", "3", null}, new String[]{"a", "b", "c", "d"});

    map.put("d", "4");
    map.put("e", "5");
    map.put("f", "6");

    Assert.assertEquals(6, map.size());

    assertMultiEqualsFromMap(map, new String[]{"4", "5", "6"}, new String[]{"d", "e", "f"});

    multiValueCheck(new String[]{"a", "b", "c", "d", "e", "f"}, ID1, new String[]{"1", "2", "3", null, null, null});

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);


    multiValueCheck(new String[]{"a", "b", "c", "d", "e", "f", "g"}, ID1, new String[]{"1", "2", "3", "4", "5", "6", null});

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
    setup();
    SpillableTimeSlicedMapImpl<String, String> map1 = new SpillableTimeSlicedMapImpl<>(store,ID1,new StringSerde(), new StringSerde(), te, ke);
    SpillableTimeSlicedMapImpl<String, String> map2 = new SpillableTimeSlicedMapImpl<>(store,ID2,new StringSerde(), new StringSerde(), te, ke);

    store.setup(testMeta.operatorContext);
    map1.setup(testMeta.operatorContext);
    map2.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    map1.beginWindow(windowId);
    map2.beginWindow(windowId);

    map1.put("a", "1");

    Assert.assertEquals("1", map1.get("a"));
    Assert.assertEquals(null, map2.get("a"));

    map2.put("a", "a1");

    Assert.assertEquals("1", map1.get("a"));
    Assert.assertEquals("a1", map2.get("a"));

    map1.put("b", "2");
    map2.put("c", "3");

    Assert.assertEquals("1", map1.get("a"));
    Assert.assertEquals("2", map1.get("b"));

    Assert.assertEquals("a1", map2.get("a"));
    Assert.assertEquals(null, map2.get("b"));
    Assert.assertEquals("3", map2.get("c"));

    map1.endWindow();
    map2.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map1.beginWindow(windowId);
    map2.beginWindow(windowId);

    multiValueCheck(new String[]{"a", "b"}, ID1, new String[]{"1", "2"});

    multiValueCheck(new String[]{"a", "b", "c"}, ID2, new String[]{"a1", null, "3"});

    map1.remove("a");

    Assert.assertEquals(null, map1.get("a"));
    Assert.assertEquals("a1", map2.get("a"));

    map1.endWindow();
    map2.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map1.beginWindow(windowId);
    map2.beginWindow(windowId);

    multiValueCheck(new String[]{"a"}, ID1, new String[]{null});
    multiValueCheck(new String[]{"a"}, ID2, new String[]{"a1"});

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
    setup();
    SpillableTimeSlicedMapImpl<String, String> map = new SpillableTimeSlicedMapImpl<>(store,ID1,new StringSerde(), new StringSerde(), te, ke);

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    store.beginWindow(0);
    map.beginWindow(0);
    map.put("x", "1");
    map.put("y", "2");
    map.put("z", "3");
    map.put("zz", "33");
    Assert.assertEquals(4, map.size());
    map.endWindow();
    store.endWindow();

    store.beginWindow(1);
    map.beginWindow(1);
    Assert.assertEquals(4, map.size());
    map.put("x", "4");
    map.put("y", "5");
    map.remove("zz");
    Assert.assertEquals(3, map.size());
    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(1);
    store.checkpointed(1);

    SpillableTimeSlicedMapImpl<String, String> clonedMap = KryoCloneUtils.cloneObject(map);

    store.beginWindow(2);
    map.beginWindow(2);
    Assert.assertEquals(3, map.size());
    map.put("x", "6");
    map.put("y", "7");
    map.put("w", "8");
    Assert.assertEquals(4, map.size());
    map.endWindow();
    store.endWindow();

    // simulating crash here
    map.teardown();
    store.teardown();

    Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_PATH, testMeta.applicationPath);
    attributes.put(Context.OperatorContext.ACTIVATION_WINDOW_ID, 1L);
    Context.OperatorContext context =
        new OperatorContextTestHelper.TestIdOperatorContext(testMeta.operatorContext.getId(), attributes);

    map = clonedMap;
    map.getStore().setup(context);
    map.setup(testMeta.operatorContext);

    map.getStore().beginWindow(2);
    map.beginWindow(2);
    Assert.assertEquals(3, map.size());
    Assert.assertEquals("4", map.get("x"));
    Assert.assertEquals("5", map.get("y"));
    Assert.assertEquals("3", map.get("z"));
    map.endWindow();
    map.getStore().endWindow();

    map.teardown();
  }

  private void multiValueCheck(String[] keys, byte[] samePrefix, String[] expectedVal)
  {
    for (int i = 0; i < keys.length; i++) {
      SpillableTimeStateTestUtils.checkValue(store, _bid(keys[i], ke), keys[i], samePrefix, expectedVal[i]);
    }
  }

  private void assertMultiEqualsFromMap(SpillableTimeSlicedMapImpl<String, String> map, String[] expectedV, String[] keys)
  {
    for (int i = 0; i < expectedV.length; i++) {
      Assert.assertEquals(expectedV[i], map.get(keys[i]));
    }
  }

  private long _bid(String key, KeyBucketExtractor te)
  {
    if (te != null) {
      return te.getBucket(key);
    } else {
      return 0L;
    }
  }
}
