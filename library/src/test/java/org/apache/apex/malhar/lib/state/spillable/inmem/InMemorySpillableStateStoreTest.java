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
package org.apache.apex.malhar.lib.state.spillable.inmem;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.util.TestUtils;

public class InMemorySpillableStateStoreTest
{
  @Test
  public void simpleStoreTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    store.setup(null);

    long windowId = 0L;
    store.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(null, store.getSync(0L, TestUtils.getSlice(1)));

    store.put(0L, TestUtils.getSlice(1), TestUtils.getSlice(2));
    store.put(0L, TestUtils.getSlice(3), TestUtils.getSlice(10));
    store.put(1L, TestUtils.getSlice(2), TestUtils.getSlice(3));
    store.put(1L, TestUtils.getSlice(4), TestUtils.getSlice(11));

    Assert.assertEquals(null, store.getSync(0L, TestUtils.getSlice(2)));
    Assert.assertEquals(null, store.getSync(0L, TestUtils.getSlice(4)));
    Assert.assertEquals(TestUtils.getSlice(2), store.getSync(0L, TestUtils.getSlice(1)));
    Assert.assertEquals(TestUtils.getSlice(10), store.getSync(0L, TestUtils.getSlice(3)));

    Assert.assertEquals(null, store.getSync(1L, TestUtils.getSlice(1)));
    Assert.assertEquals(null, store.getSync(1L, TestUtils.getSlice(3)));
    Assert.assertEquals(TestUtils.getSlice(3), store.getSync(1L, TestUtils.getSlice(2)));
    Assert.assertEquals(TestUtils.getSlice(11), store.getSync(1L, TestUtils.getSlice(4)));

    store.endWindow();

    store.teardown();
  }
}
