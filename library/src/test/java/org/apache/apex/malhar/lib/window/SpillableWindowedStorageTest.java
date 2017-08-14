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
package org.apache.apex.malhar.lib.window;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponentImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableTestUtils;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.apex.malhar.lib.window.impl.SpillableWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.SpillableWindowedPlainStorage;

import com.datatorrent.api.Context;

/**
 * Unit tests for Spillable Windowed Storage
 */
public class SpillableWindowedStorageTest
{
  @Rule
  public SpillableTestUtils.TestMeta testMeta = new SpillableTestUtils.TestMeta();

  public static long BASETIME = System.currentTimeMillis();

  @Test
  public void testWindowedPlainStorage()
  {
    SpillableComplexComponentImpl sccImpl = new SpillableComplexComponentImpl(testMeta.timeStore);
    SpillableWindowedPlainStorage<Integer> storage = new SpillableWindowedPlainStorage<>();
    Window window1 = new Window.TimeWindow<>(BASETIME + 1000, 10);
    Window window2 = new Window.TimeWindow<>(BASETIME + 1010, 10);
    Window window3 = new Window.TimeWindow<>(BASETIME + 1020, 10);
    storage.setSpillableComplexComponent(sccImpl);

    /*
     * storage.setup() will create Spillable Data Structures
     * storage.getSpillableComplexComponent().setup() will setup these Data Structures.
     * So storage.setup() should be called before storage.getSpillableComplexComponent().setup()
     */
    storage.setup(testMeta.operatorContext);
    storage.getSpillableComplexComponent().setup(testMeta.operatorContext);

    sccImpl.beginWindow(1000);
    storage.put(window1, 1);
    storage.put(window2, 2);
    storage.put(window3, 3);
    sccImpl.endWindow();
    sccImpl.beginWindow(1001);
    storage.put(window1, 4);
    storage.put(window2, 5);
    sccImpl.endWindow();
    sccImpl.beforeCheckpoint(1001);
    SpillableWindowedPlainStorage<Integer> clonedStorage = KryoCloneUtils.cloneObject(storage);
    sccImpl.checkpointed(1001);


    sccImpl.beginWindow(1002);
    storage.put(window1, 6);
    storage.put(window2, 7);
    sccImpl.endWindow();

    Assert.assertEquals(6L, storage.get(window1).longValue());
    Assert.assertEquals(7L, storage.get(window2).longValue());
    Assert.assertEquals(3L, storage.get(window3).longValue());

    sccImpl.beginWindow(1003);
    storage.put(window1, 8);
    storage.put(window2, 9);
    sccImpl.endWindow();

    // simulating crash here
    storage.teardown();
    storage.getSpillableComplexComponent().teardown();

    storage = clonedStorage;
    testMeta.operatorContext.getAttributes().put(Context.OperatorContext.ACTIVATION_WINDOW_ID, 1001L);
    storage.getSpillableComplexComponent().setup(testMeta.operatorContext);
    storage.setup(testMeta.operatorContext);

    // recovery at window 1002
    sccImpl.beginWindow(1002);
    Assert.assertEquals(4L, storage.get(window1).longValue());
    Assert.assertEquals(5L, storage.get(window2).longValue());
    Assert.assertEquals(3L, storage.get(window3).longValue());
  }

  @Test
  public void testWindowedKeyedStorage()
  {
    SpillableComplexComponentImpl sccImpl = new SpillableComplexComponentImpl(testMeta.timeStore);
    SpillableWindowedKeyedStorage<String, Integer> storage = new SpillableWindowedKeyedStorage<>();
    Window window1 = new Window.TimeWindow<>(BASETIME + 1000, 10);
    Window window2 = new Window.TimeWindow<>(BASETIME + 1010, 10);
    Window window3 = new Window.TimeWindow<>(BASETIME + 1020, 10);
    storage.setSpillableComplexComponent(sccImpl);

    /*
     * storage.setup() will create Spillable Data Structures
     * storage.getSpillableComplexComponent().setup() will setup these Data Structures.
     * So storage.setup() should be called before storage.getSpillableComplexComponent().setup()
     */
    storage.setup(testMeta.operatorContext);
    storage.getSpillableComplexComponent().setup(testMeta.operatorContext);

    sccImpl.beginWindow(1000);
    storage.put(window1, "x", 1);
    storage.put(window2, "x", 2);
    storage.put(window3, "x", 3);
    sccImpl.endWindow();
    sccImpl.beginWindow(1001);
    storage.put(window1, "x", 4);
    storage.put(window2, "x", 5);
    sccImpl.endWindow();
    sccImpl.beforeCheckpoint(1001);
    SpillableWindowedKeyedStorage<String, Integer> clonedStorage = KryoCloneUtils.cloneObject(storage);
    sccImpl.checkpointed(1001);

    sccImpl.beginWindow(1002);
    storage.put(window1, "x", 6);
    storage.put(window2, "x", 7);
    storage.put(window2, "y", 8);
    sccImpl.endWindow();

    Assert.assertEquals(6L, storage.get(window1, "x").longValue());
    Assert.assertEquals(7L, storage.get(window2, "x").longValue());
    Assert.assertEquals(3L, storage.get(window3, "x").longValue());
    Assert.assertEquals(8L, storage.get(window2, "y").longValue());

    // simulating crash here
    storage.teardown();
    storage.getSpillableComplexComponent().teardown();

    storage = clonedStorage;
    testMeta.operatorContext.getAttributes().put(Context.OperatorContext.ACTIVATION_WINDOW_ID, 1001L);
    storage.getSpillableComplexComponent().setup(testMeta.operatorContext);
    storage.setup(testMeta.operatorContext);

    // recovery at window 1002
    sccImpl.beginWindow(1002);
    Assert.assertEquals(4L, storage.get(window1, "x").longValue());
    Assert.assertEquals(5L, storage.get(window2, "x").longValue());
    Assert.assertEquals(3L, storage.get(window3, "x").longValue());
    Assert.assertNull(storage.get(window2, "y"));

  }
}
