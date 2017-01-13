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

import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.utils.serde.StringSerde;
import com.google.common.collect.Lists;

public class SpillableTimeSlicedArrayListImplTest
{
  public static final byte[] ID1 = new byte[]{(byte)0};
  public static final byte[] ID2 = new byte[]{(byte)1};

  private SpillableTimeStateStore store;

  private TimeExtractor<String> te = null;

  @Rule
  public SpillableTimeStateTestUtils.TestMeta testMeta = new SpillableTimeStateTestUtils.TestMeta();

  private void setup()
  {
    store = testMeta.store;
    te = new TestStringTimeExtractor();
  }

  @Test
  public void simpleAddGetAndSetTest1Helper()
  {
    setup();
    SpillableTimeSlicedArrayListImpl<String> list = new SpillableTimeSlicedArrayListImpl<String>(0L, ID1, store,
        new StringSerde(), te);

    store.setup(testMeta.operatorContext);
    list.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    SpillableTimeStateTestUtils.checkOutOfBounds(list, 0);
    Assert.assertEquals(0, list.size());

    list.add("a");

    SpillableTimeStateTestUtils.checkOutOfBounds(list, 1);
    Assert.assertEquals(1, list.size());

    Assert.assertEquals("a", list.get(0));

    list.addAll(Lists.newArrayList("a", "b", "c"));

    Assert.assertEquals(4, list.size());

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("a", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("c", list.get(3));

    SpillableTimeStateTestUtils.checkOutOfBounds(list, 4);

    list.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    list.beginWindow(windowId);

    SpillableTimeStateTestUtils.checkValue(store, 0L, ID1, 0, Lists.newArrayList("a", "a", "b", "c"));

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

    SpillableTimeStateTestUtils.checkValue(store, 0L, ID1, 0, Lists.newArrayList("a", "111", "b", "c", "tt", "ab", "99", "oo"));
    list.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    list.teardown();
    store.teardown();
  }
}
