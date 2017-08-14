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
package org.apache.apex.malhar.lib.window.impl;

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.accumulation.CoGroup;

import com.google.common.base.Function;

/**
 * Unit tests for Windowed Join Operator
 */
public class WindowedMergeOperatorTest
{
  @Test
  public void extractTimestampTest()
  {
    WindowedMergeOperatorImpl op = createDefaultWindowedMergeOperator();
    Function<Integer, Long> timestampExtractor = new Function<Integer, Long>()
    {
      @Override
      public Long apply(Integer input)
      {
        return (input * 10L);
      }
    };

    Assert.assertEquals(1000L, op.extractTimestamp(new Tuple.PlainTuple<Integer>(100), timestampExtractor));
    Assert.assertEquals(2000L, op.extractTimestamp(new Tuple.PlainTuple<Integer>(200), timestampExtractor));
    Assert.assertEquals(200L, op.extractTimestamp(new Tuple.TimestampedTuple<Integer>(200L, 10), null));
  }


  @Test
  public void windowedMergeOperatorMergeTest()
  {
    WindowedMergeOperatorImpl<Integer, Integer, List<Set<Integer>>, List<List<Integer>>> op = createDefaultWindowedMergeOperator();
    Window global = Window.GlobalWindow.INSTANCE;
    op.setDataStorage(new InMemoryWindowedStorage<List<Set<Integer>>>());
    op.setWindowOption(new WindowOption.GlobalWindow());
    op.initializeWindowStates(AbstractWindowedOperator.GLOBAL_WINDOW_SINGLETON_SET);

    op.processTuple(new Tuple.WindowedTuple<Integer>(global, 100));
    Assert.assertEquals(1, op.dataStorage.get(global).get(0).size());
    op.processTuple2(new Tuple.WindowedTuple<Integer>(global, 200));
    Assert.assertEquals(1, op.dataStorage.get(global).get(1).size());
    op.processTuple(new Tuple.WindowedTuple<Integer>(global, 300));
    Assert.assertEquals(2, op.dataStorage.get(global).get(0).size());
    Assert.assertEquals(2, op.accumulation.getOutput(op.dataStorage.get(global)).size());
  }

  @Test
  public void keyedWindowedMergeOperatorMergeTest()
  {
    KeyedWindowedMergeOperatorImpl<String, Integer, Integer, List<Set<Integer>>, List<List<Integer>>> op
        = createDefaultKeyedWindowedMergeOperator();
    Window global = Window.GlobalWindow.INSTANCE;
    op.setDataStorage(new InMemoryWindowedKeyedStorage<String, List<Set<Integer>>>());
    op.setWindowOption(new WindowOption.GlobalWindow());
    op.initializeWindowStates(AbstractWindowedOperator.GLOBAL_WINDOW_SINGLETON_SET);

    op.processTuple(new Tuple.WindowedTuple<KeyValPair<String, Integer>>(global, new KeyValPair<String, Integer>("A", 100)));
    Assert.assertEquals(1, op.dataStorage.get(global, "A").get(0).size());
    Assert.assertTrue(op.dataStorage.get(global, "A").get(0).contains(100));

    op.processTuple2(new Tuple.WindowedTuple<KeyValPair<String, Integer>>(global, new KeyValPair<String, Integer>("A", 200)));
    Assert.assertEquals(1, op.dataStorage.get(global, "A").get(1).size());
    Assert.assertTrue(op.dataStorage.get(global, "A").get(1).contains(200));

    op.processTuple2(new Tuple.WindowedTuple<KeyValPair<String, Integer>>(global, new KeyValPair<String, Integer>("B", 300)));
    Assert.assertEquals(1, op.dataStorage.get(global, "A").get(1).size());
    Assert.assertEquals(1, op.dataStorage.get(global, "B").get(1).size());
    Assert.assertTrue(op.dataStorage.get(global, "B").get(1).contains(300));

    Assert.assertEquals(2, op.accumulation.getOutput(op.dataStorage.get(global, "A")).size());
  }

  @Test
  public void windowedMergeOperatorWatermarkTest()
  {
    WindowedMergeOperatorImpl op = createDefaultWindowedMergeOperator();
    CollectorTestSink<WatermarkImpl> sink = new CollectorTestSink<>();
    op.controlOutput.setSink(sink);

    // No watermark is generated if the Merge operator haven't seen all watermarks from all input streams.
    op.controlInput.process(new WatermarkImpl(1000000));
    op.endWindow();
    Assert.assertEquals(-1, op.currentWatermark);
    Assert.assertEquals(0, sink.collectedTuples.size());

    // Once both input streams sent watermarks to Merge operator, it should generate a watermark and send to downstream.
    op.controlInput2.process(new WatermarkImpl(200000));
    op.endWindow();
    Assert.assertEquals(200000, op.currentWatermark);
    Assert.assertEquals(1, sink.collectedTuples.size());

    // If the minimum of the latest input watermarks changes, Merge operator should also generate a new watermark.
    op.controlInput2.process(new WatermarkImpl(2100000));
    op.endWindow();
    Assert.assertEquals(1000000, op.currentWatermark);
    Assert.assertEquals(2, sink.collectedTuples.size());

    // Current watermark of Merge operator could only change during endWindow() event.
    op.controlInput.process(new WatermarkImpl(1100000));
    op.endWindow();
    Assert.assertEquals(1100000, op.currentWatermark);
    Assert.assertEquals(3, sink.collectedTuples.size());

    // If the upstreams sent a watermark but the minimum of the latest input watermarks doesn't change, the Merge
    // operator should not generate a new watermark, thus nothing will be sent to downstream.
    op.controlInput.process(new WatermarkImpl(1100000));
    op.endWindow();
    Assert.assertEquals(1100000, op.currentWatermark);
    Assert.assertEquals(3, sink.collectedTuples.size());
  }

  private WindowedMergeOperatorImpl<Integer, Integer, List<Set<Integer>>, List<List<Integer>>> createDefaultWindowedMergeOperator()
  {
    WindowedMergeOperatorImpl<Integer, Integer, List<Set<Integer>>, List<List<Integer>>> windowedMergeOperator = new WindowedMergeOperatorImpl<>();
    windowedMergeOperator.setDataStorage(new InMemoryWindowedStorage<List<Set<Integer>>>());
    windowedMergeOperator.setRetractionStorage(new InMemoryWindowedStorage<List<List<Integer>>>());
    windowedMergeOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowedMergeOperator.setAccumulation(new CoGroup<Integer>());
    return windowedMergeOperator;
  }

  private KeyedWindowedMergeOperatorImpl<String, Integer, Integer, List<Set<Integer>>, List<List<Integer>>> createDefaultKeyedWindowedMergeOperator()
  {
    KeyedWindowedMergeOperatorImpl<String, Integer, Integer, List<Set<Integer>>, List<List<Integer>>> windowedMergeOperator = new KeyedWindowedMergeOperatorImpl<>();
    windowedMergeOperator.setDataStorage(new InMemoryWindowedKeyedStorage<String, List<Set<Integer>>>());
    windowedMergeOperator.setRetractionStorage(new InMemoryWindowedKeyedStorage<String, List<List<Integer>>>());
    windowedMergeOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowedMergeOperator.setAccumulation(new CoGroup<Integer>());
    return windowedMergeOperator;
  }

}
