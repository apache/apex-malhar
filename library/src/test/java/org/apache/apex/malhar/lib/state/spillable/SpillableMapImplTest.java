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
import org.junit.runner.RunWith;

import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.apex.malhar.lib.utils.serde.AffixKeyValueSerdeManager;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerializationBuffer;
import org.apache.apex.malhar.lib.utils.serde.StringSerde;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

@RunWith(JUnitParamsRunner.class)
public class SpillableMapImplTest
{
  public static final byte[] ID1 = new byte[]{(byte)0};
  public static final byte[] ID2 = new byte[]{(byte)1};

  public static final TestStringTimeExtractor TE = new TestStringTimeExtractor();

  private SpillableStateStore store;

  private TimeExtractor<String> te = null;


  @Rule
  public SpillableTestUtils.TestMeta testMeta = new SpillableTestUtils.TestMeta();


  private void setup(String opt)
  {
    if (opt.equals("InMem")) {
      store = new InMemSpillableStateStore();
      te = null;
    } else if (opt.equals("ManagedState")) {
      store = testMeta.store;
      te = null;
    } else {
      store = testMeta.timeStore;
      te = TE;
    }
  }


  @Test
  @Parameters({"InMem","ManagedState","TimeUnifiedManagedState"})
  public void simpleGetAndPutTest(String opt)
  {
    setup(opt);
    SpillableMapImpl<String, String> map = createSpillableMap(store);

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

  private void multiValueCheck(String[] keys, byte[] samePrefix, String[] expectedVal)
  {
    for (int i = 0; i < keys.length; i++) {
      SpillableTestUtils.checkValue(store, _bid(keys[i], te), keys[i], samePrefix, expectedVal[i]);
    }
  }

  private void assertMultiEqualsFromMap(SpillableMapImpl<String, String> map, String[] expectedV, String[] keys)
  {
    for (int i = 0; i < expectedV.length; i++) {
      Assert.assertEquals(expectedV[i], map.get(keys[i]));
    }
  }

  private long _bid(String key, TimeExtractor<String> te)
  {
    if (te != null) {
      return te.getTime(key);
    } else {
      return 0L;
    }
  }

  private SpillableMapImpl<String, String> createSpillableMap(SpillableStateStore store)
  {
    if (te == null) {
      return new SpillableMapImpl<>(store,ID1,0L,new StringSerde(), new StringSerde());
    } else {
      return new SpillableMapImpl<>(store,ID1,new StringSerde(), new StringSerde(), te);
    }
  }

  @Test
  @Parameters({"InMem","ManagedState","TimeUnifiedManagedState"})
  public void simpleRemoveTest(String opt)
  {
    setup(opt);
    SpillableMapImpl<String, String> map = createSpillableMap(store);

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

    map.remove("b");
    map.remove("c");

    Assert.assertEquals("1", map.get("a"));
    Assert.assertEquals(null, map.get("b"));
    Assert.assertEquals(null, map.get("c"));
    Assert.assertEquals(null, map.get("d"));

    Assert.assertEquals(1, map.size());

    multiValueCheck(new String[]{"a", "b", "c", "d"}, ID1, new String[]{null, null, null, null});

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    multiValueCheck(new String[]{"a", "b", "c", "d"}, ID1, new String[]{"1", null, null, null});

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    Assert.assertEquals(1, map.size());

    Assert.assertEquals("1", map.get("a"));
    Assert.assertEquals(null, map.get("b"));
    Assert.assertEquals(null, map.get("c"));
    Assert.assertEquals(null, map.get("d"));

    map.put("d", "4");
    map.put("e", "5");
    map.put("f", "6");

    Assert.assertEquals(4, map.size());

    Assert.assertEquals("4", map.get("d"));
    Assert.assertEquals("5", map.get("e"));
    Assert.assertEquals("6", map.get("f"));

    multiValueCheck(new String[]{"a", "b", "c", "d", "e", "f"}, ID1, new String[]{"1", null, null, null, null, null});

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    multiValueCheck(new String[]{"a", "b", "c", "d", "e", "f", "g"}, ID1, new String[]{"1", null, null, "4", "5", "6", null});

    map.remove("a");
    map.remove("d");
    Assert.assertEquals(null, map.get("a"));
    Assert.assertEquals(null, map.get("b"));
    Assert.assertEquals(null, map.get("c"));
    Assert.assertEquals(null, map.get("d"));
    Assert.assertEquals("5", map.get("e"));
    Assert.assertEquals("6", map.get("f"));
    Assert.assertEquals(null, map.get("g"));

    multiValueCheck(new String[]{"a", "b", "c", "d", "e", "f", "g"}, ID1, new String[]{"1", null, null, "4", "5", "6", null});

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    multiValueCheck(new String[]{"a", "b", "c", "d", "e", "f", "g"}, ID1, new String[]{null, null, null, null, "5", "6", null});

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    map.teardown();
    store.teardown();
  }

  @Test
  @Parameters({"InMem","ManagedState","TimeUnifiedManagedState"})
  public void multiMapPerBucketTest(String opt)
  {
    setup(opt);
    StringSerde sss = new StringSerde();

    SpillableMapImpl<String, String> map1 = null;
    SpillableMapImpl<String, String> map2 = null;
    if (te == null) {
      map1 = new SpillableMapImpl<>(store, ID1, 0L, sss, sss);
      map2 = new SpillableMapImpl<>(store, ID2, 0L, sss, sss);
    } else {
      map1 = new SpillableMapImpl<>(store, ID1, sss, sss, te);
      map2 = new SpillableMapImpl<>(store, ID2, sss, sss, te);
    }

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
  @Parameters({"ManagedState","TimeUnifiedManagedState"})
  public void recoveryWithManagedStateTest(String opt) throws Exception
  {
    setup(opt);
    StringSerde sss = new StringSerde();
    SpillableMapImpl<String, String> map1 = null;
    if (te == null) {
      map1 = new SpillableMapImpl<>(store, ID1, 0L, sss, sss);
    } else {
      map1 = new SpillableMapImpl<>(store, ID1, sss, sss, te);
    }

    store.setup(testMeta.operatorContext);
    map1.setup(testMeta.operatorContext);

    store.beginWindow(0);
    map1.beginWindow(0);
    map1.put("x", "1");
    map1.put("y", "2");
    map1.put("z", "3");
    map1.put("zz", "33");
    Assert.assertEquals(4, map1.size());
    map1.endWindow();
    store.endWindow();

    store.beginWindow(1);
    map1.beginWindow(1);
    Assert.assertEquals(4, map1.size());
    map1.put("x", "4");
    map1.put("y", "5");
    map1.remove("zz");
    Assert.assertEquals(3, map1.size());
    map1.endWindow();
    store.endWindow();
    store.beforeCheckpoint(1);
    store.checkpointed(1);

    SpillableMapImpl<String, String> clonedMap1 = KryoCloneUtils.cloneObject(map1);

    store.beginWindow(2);
    map1.beginWindow(2);
    Assert.assertEquals(3, map1.size());
    map1.put("x", "6");
    map1.put("y", "7");
    map1.put("w", "8");
    Assert.assertEquals(4, map1.size());
    map1.endWindow();
    store.endWindow();

    // simulating crash here
    map1.teardown();
    store.teardown();

    Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_PATH, testMeta.applicationPath);
    attributes.put(Context.OperatorContext.ACTIVATION_WINDOW_ID, 1L);
    OperatorContext context = mockOperatorContext(testMeta.operatorContext.getId(), attributes);

    map1 = clonedMap1;
    map1.getStore().setup(context);
    map1.setup(testMeta.operatorContext);

    map1.getStore().beginWindow(2);
    map1.beginWindow(2);
    Assert.assertEquals(3, map1.size());
    Assert.assertEquals("4", map1.get("x"));
    Assert.assertEquals("5", map1.get("y"));
    Assert.assertEquals("3", map1.get("z"));
    map1.endWindow();
    map1.getStore().endWindow();

    map1.teardown();
  }


  protected static class SerdeManagerForTest<K, V> extends AffixKeyValueSerdeManager<K, V>
  {
    public SerdeManagerForTest(byte[] metaKeySuffix, byte[] dataKeyIdentifier, Serde<K> keySerde, Serde<V> valueSerde)
    {
      super(metaKeySuffix, dataKeyIdentifier, keySerde, valueSerde);
    }

    public SerializationBuffer getValueBuffer()
    {
      return valueBuffer;
    }

    public SerializationBuffer getKeyBufferForWrite()
    {
      return keyBufferForWrite;
    }
  }

  protected static class SpillableMapImplForTest<K, V> extends SpillableMapImpl<K, V>
  {
    protected SerdeManagerForTest<K, V> serdeManager;

    public SpillableMapImplForTest(SpillableStateStore store, byte[] identifier, long bucket, Serde<K> serdeKey,
        Serde<V> serdeValue)
    {
      super(store, identifier, bucket, serdeKey, serdeValue);
      serdeManager = new SerdeManagerForTest<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
      keyValueSerdeManager = serdeManager;
    }

    public SpillableMapImplForTest(SpillableStateStore store, byte[] identifier, Serde<K> serdeKey,
        Serde<V> serdeValue, TimeExtractor<K> timeExtractor)
    {
      super(store, identifier, serdeKey, serdeValue, timeExtractor);
      serdeManager = new SerdeManagerForTest<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
      keyValueSerdeManager = serdeManager;
    }
  }

  @Test
  @Parameters({"TimeUnifiedManagedState"})
  public void serializationBufferTest(String opt)
  {
    SerializationBuffer keyBuffer = null;
    SerializationBuffer valueBuffer = null;
    SerializationBuffer currentBuffer;

    setup(opt);
    SpillableMapImplForTest<String, String> map;
    if (te == null) {
      map = new SpillableMapImplForTest<>(store,ID1,0L,new StringSerde(), new StringSerde());
    } else {
      map = new SpillableMapImplForTest<>(store,ID1,new StringSerde(), new StringSerde(), te);
    }

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    map.put("a", "1");

    map.endWindow();
    store.endWindow();

    currentBuffer = map.serdeManager.getKeyBufferForWrite();
    Assert.assertTrue(currentBuffer != keyBuffer);
    keyBuffer = currentBuffer;

    currentBuffer = map.serdeManager.getValueBuffer();
    Assert.assertTrue(currentBuffer != valueBuffer);
    valueBuffer = currentBuffer;

    ++windowId;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    //each put use different key to make sure use the different bucket
    map.put("b", "2");

    map.endWindow();
    store.endWindow();

    currentBuffer = map.serdeManager.getKeyBufferForWrite();
    Assert.assertTrue(currentBuffer != keyBuffer);
    keyBuffer = currentBuffer;

    currentBuffer = map.serdeManager.getValueBuffer();
    Assert.assertTrue(currentBuffer != valueBuffer);
    valueBuffer = currentBuffer;

    map.teardown();
    store.teardown();
  }
}
