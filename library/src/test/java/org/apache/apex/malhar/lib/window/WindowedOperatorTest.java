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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.ValidationException;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.apex.malhar.lib.window.impl.WatermarkImpl;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;
import org.apache.commons.lang3.mutable.MutableLong;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Sink;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Unit tests for WindowedOperator
 */
public class WindowedOperatorTest
{
  private void verifyValidationFailure(WindowedOperatorImpl windowedOperator, String message)
  {
    try {
      windowedOperator.validate();
      Assert.fail("Should fail validation because " + message);
    } catch (ValidationException ex) {
      return;
    }
  }

  private WindowedOperatorImpl<Long, MutableLong, Long> createDefaultWindowedOperator()
  {
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = new WindowedOperatorImpl<>();
    windowedOperator.setDataStorage(new InMemoryWindowedStorage<MutableLong>());
    windowedOperator.setRetractionStorage(new InMemoryWindowedStorage<Long>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowedOperator.setAccumulation(new SumAccumulation());
    return windowedOperator;
  }

  private KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> createDefaultKeyedWindowedOperator()
  {
    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> windowedOperator = new KeyedWindowedOperatorImpl<>();
    windowedOperator.setDataStorage(new InMemoryWindowedKeyedStorage<String, MutableLong>());
    windowedOperator.setRetractionStorage(new InMemoryWindowedKeyedStorage<String, Long>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowedOperator.setAccumulation(new SumAccumulation());
    return windowedOperator;
  }

  @Test
  public void testValidation() throws Exception
  {
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = new WindowedOperatorImpl<>();
    verifyValidationFailure(windowedOperator, "nothing is configured");
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    verifyValidationFailure(windowedOperator, "data storage is not set");
    windowedOperator.setDataStorage(new InMemoryWindowedStorage<MutableLong>());
    verifyValidationFailure(windowedOperator, "accumulation is not set");
    windowedOperator.setAccumulation(new SumAccumulation());
    windowedOperator.validate();
    windowedOperator.setTriggerOption(new TriggerOption().accumulatingAndRetractingFiredPanes());
    verifyValidationFailure(windowedOperator, "retracting storage is not set for ACCUMULATING_AND_RETRACTING");
    windowedOperator.setRetractionStorage(new InMemoryWindowedStorage<Long>());
    windowedOperator.validate();
    windowedOperator.setTriggerOption(new TriggerOption().discardingFiredPanes().firingOnlyUpdatedPanes());
    verifyValidationFailure(windowedOperator, "DISCARDING is not valid for option firingOnlyUpdatedPanes");
    windowedOperator.setTriggerOption(new TriggerOption().accumulatingFiredPanes().firingOnlyUpdatedPanes());
    windowedOperator.setRetractionStorage(null);
    verifyValidationFailure(windowedOperator, "retracting storage is not set for option firingOnlyUpdatedPanes");
  }

  @Test
  public void testWatermarkAndAllowedLateness()
  {
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(1,
        new Attribute.AttributeMap.DefaultAttributeMap());
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createDefaultWindowedOperator();
    CollectorTestSink controlSink = new CollectorTestSink();

    windowedOperator.controlOutput.setSink(controlSink);

    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1000)));
    windowedOperator.setAllowedLateness(Duration.millis(1000));

    WindowedStorage<MutableLong> dataStorage = new InMemoryWindowedStorage<>();
    WindowedStorage<WindowState> windowStateStorage = new InMemoryWindowedStorage<>();

    windowedOperator.setDataStorage(dataStorage);
    windowedOperator.setWindowStateStorage(windowStateStorage);

    windowedOperator.setup(context);
    windowedOperator.beginWindow(1);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(100L, 2L));
    Assert.assertEquals("There should be exactly one window in the storage", 1, dataStorage.size());
    Assert.assertEquals("There should be exactly one window in the storage", 1, windowStateStorage.size());

    Map.Entry<Window, WindowState> entry = windowStateStorage.entrySet().iterator().next();
    Window window = entry.getKey();
    WindowState windowState = entry.getValue();
    Assert.assertEquals(-1, windowState.watermarkArrivalTime);
    Assert.assertEquals(2L, dataStorage.get(window).longValue());

    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(200L, 3L));
    Assert.assertEquals(5L, dataStorage.get(window).longValue());

    windowedOperator.processWatermark(new WatermarkImpl(1200));
    windowedOperator.endWindow();
    Assert.assertTrue(windowState.watermarkArrivalTime > 0);
    Assert.assertEquals("We should get one watermark tuple", 1, controlSink.getCount(false));

    windowedOperator.beginWindow(2);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(900L, 4L));
    Assert.assertEquals("Late but not too late", 9L, dataStorage.get(window).longValue());
    windowedOperator.processWatermark(new WatermarkImpl(3000));
    windowedOperator.endWindow();
    Assert.assertEquals("We should get two watermark tuples", 2, controlSink.getCount(false));
    windowedOperator.beginWindow(3);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(120L, 5L)); // this tuple should be dropped
    Assert.assertEquals("The window should be dropped because it's too late", 0, dataStorage.size());
    Assert.assertEquals("The window should be dropped because it's too late", 0, windowStateStorage.size());
    windowedOperator.endWindow();
  }

  private void testTrigger(TriggerOption.AccumulationMode accumulationMode)
  {
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createDefaultWindowedOperator();
    TriggerOption triggerOption = new TriggerOption().withEarlyFiringsAtEvery(Duration.millis(1000));
    switch (accumulationMode) {
      case ACCUMULATING:
        triggerOption.accumulatingFiredPanes();
        break;
      case ACCUMULATING_AND_RETRACTING:
        triggerOption.accumulatingAndRetractingFiredPanes();
        break;
      case DISCARDING:
        triggerOption.discardingFiredPanes();
        break;
      default:
        throw new RuntimeException("Unknown accumulation mode: " + accumulationMode);
    }
    windowedOperator.setTriggerOption(triggerOption);
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1000)));
    CollectorTestSink sink = new CollectorTestSink();
    windowedOperator.output.setSink(sink);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(1,
        new Attribute.AttributeMap.DefaultAttributeMap());
    windowedOperator.setup(context);
    windowedOperator.beginWindow(1);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(100L, 2L));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(200L, 3L));
    windowedOperator.endWindow();
    Assert.assertTrue("No trigger should be fired yet", sink.collectedTuples.isEmpty());
    windowedOperator.beginWindow(2);
    windowedOperator.endWindow();
    Assert.assertTrue("No trigger should be fired yet", sink.collectedTuples.isEmpty());
    windowedOperator.beginWindow(3);
    windowedOperator.endWindow();
    Assert.assertEquals("There should be exactly one tuple for the time trigger", 1, sink.collectedTuples.size());
    Assert.assertEquals(5L, ((Tuple<Long>)sink.collectedTuples.get(0)).getValue().longValue());
    sink.collectedTuples.clear();
    windowedOperator.beginWindow(4);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(400L, 4L));
    windowedOperator.endWindow();
    Assert.assertTrue("No trigger should be fired yet", sink.collectedTuples.isEmpty());
    windowedOperator.beginWindow(5);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(300L, 5L));
    windowedOperator.endWindow();
    switch (accumulationMode) {
      case ACCUMULATING:
        Assert.assertEquals("There should be exactly one tuple for the time trigger", 1, sink.collectedTuples.size());
        Assert.assertEquals(14L, ((Tuple<Long>)sink.collectedTuples.get(0)).getValue().longValue());
        break;
      case DISCARDING:
        Assert.assertEquals("There should be exactly one tuple for the time trigger", 1, sink.collectedTuples.size());
        Assert.assertEquals(9L, ((Tuple<Long>)sink.collectedTuples.get(0)).getValue().longValue());
        break;
      case ACCUMULATING_AND_RETRACTING:
        Assert.assertEquals("There should be exactly two tuples for the time trigger", 2, sink.collectedTuples.size());
        Assert.assertEquals(-5L, ((Tuple<Long>)sink.collectedTuples.get(0)).getValue().longValue());
        Assert.assertEquals(14L, ((Tuple<Long>)sink.collectedTuples.get(1)).getValue().longValue());
        break;
      default:
        throw new RuntimeException("Unknown accumulation mode: " + accumulationMode);
    }
  }

  @Test
  public void testTriggerWithDiscardingMode()
  {
    testTrigger(TriggerOption.AccumulationMode.DISCARDING);
  }

  @Test
  public void testTriggerWithAccumulatingMode()
  {
    testTrigger(TriggerOption.AccumulationMode.ACCUMULATING);
  }

  @Test
  public void testTriggerWithAccumulatingAndRetractingMode()
  {
    testTrigger(TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING);
  }

  @Test
  public void testTriggerWithAccumulatingModeFiringOnlyUpdatedPanes()
  {
    for (boolean firingOnlyUpdatedPanes : new boolean[]{true, false}) {
      WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createDefaultWindowedOperator();
      TriggerOption triggerOption = new TriggerOption().withEarlyFiringsAtEvery(Duration.millis(1000))
          .accumulatingFiredPanes();
      if (firingOnlyUpdatedPanes) {
        triggerOption.firingOnlyUpdatedPanes();
      }
      windowedOperator.setTriggerOption(triggerOption);
      windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1000)));
      CollectorTestSink sink = new CollectorTestSink();
      windowedOperator.output.setSink(sink);
      OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(1,
          new Attribute.AttributeMap.DefaultAttributeMap());
      windowedOperator.setup(context);
      windowedOperator.beginWindow(1);
      windowedOperator.processTuple(new Tuple.TimestampedTuple<>(100L, 2L));
      windowedOperator.processTuple(new Tuple.TimestampedTuple<>(200L, 3L));
      windowedOperator.endWindow();
      Assert.assertTrue("No trigger should be fired yet", sink.collectedTuples.isEmpty());
      windowedOperator.beginWindow(2);
      windowedOperator.endWindow();
      Assert.assertTrue("No trigger should be fired yet", sink.collectedTuples.isEmpty());
      windowedOperator.beginWindow(3);
      windowedOperator.endWindow();
      Assert.assertEquals("There should be exactly one tuple for the time trigger", 1, sink.collectedTuples.size());
      Assert.assertEquals(5L, ((Tuple<Long>)sink.collectedTuples.get(0)).getValue().longValue());
      sink.collectedTuples.clear();
      windowedOperator.beginWindow(4);
      windowedOperator.endWindow();
      Assert.assertTrue("No trigger should be fired yet", sink.collectedTuples.isEmpty());
      windowedOperator.beginWindow(5);
      windowedOperator.endWindow();
      if (firingOnlyUpdatedPanes) {
        Assert.assertTrue("There should not be any trigger since no panes have been updated", sink.collectedTuples.isEmpty());
      } else {
        Assert.assertEquals("There should be exactly one tuple for the time trigger", 1, sink.collectedTuples.size());
        Assert.assertEquals(5L, ((Tuple<Long>)sink.collectedTuples.get(0)).getValue().longValue());
      }
    }
  }

  @Test
  public void testGlobalWindowAssignment()
  {
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(1,
        new Attribute.AttributeMap.DefaultAttributeMap());
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createDefaultWindowedOperator();
    windowedOperator.setWindowOption(new WindowOption.GlobalWindow());
    windowedOperator.setup(context);
    Tuple.WindowedTuple<Long> windowedValue = windowedOperator.getWindowedValue(new Tuple.TimestampedTuple<>(1100L, 2L));
    List<Window> windows = windowedValue.getWindows();
    Assert.assertEquals(1, windows.size());
    Assert.assertEquals(Window.GLOBAL_WINDOW, windows.get(0));
  }

  @Test
  public void testTimeWindowAssignment()
  {
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(1,
        new Attribute.AttributeMap.DefaultAttributeMap());
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createDefaultWindowedOperator();
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1000)));
    windowedOperator.setup(context);
    Tuple.WindowedTuple<Long> windowedValue = windowedOperator.getWindowedValue(new Tuple.TimestampedTuple<>(1100L, 2L));
    List<Window> windows = windowedValue.getWindows();
    Assert.assertEquals(1, windows.size());
    Assert.assertEquals(1000, windows.get(0).getBeginTimestamp());
    Assert.assertEquals(1000, windows.get(0).getDurationMillis());
  }

  @Test
  public void testSlidingWindowAssignment()
  {
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(1,
        new Attribute.AttributeMap.DefaultAttributeMap());
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createDefaultWindowedOperator();
    windowedOperator.setWindowOption(new WindowOption.SlidingTimeWindows(Duration.millis(1000), Duration.millis(200)));
    windowedOperator.setup(context);
    Tuple.WindowedTuple<Long> windowedValue = windowedOperator.getWindowedValue(new Tuple.TimestampedTuple<>(1600L, 2L));
    List<Window> windows = windowedValue.getWindows();
    Window[] winArray = windows.toArray(new Window[]{});
    Arrays.sort(winArray, Window.DEFAULT_COMPARATOR);
    Assert.assertEquals(5, winArray.length);
    Assert.assertEquals(800, winArray[0].getBeginTimestamp());
    Assert.assertEquals(1000, winArray[0].getDurationMillis());
    Assert.assertEquals(1000, winArray[1].getBeginTimestamp());
    Assert.assertEquals(1000, winArray[1].getDurationMillis());
    Assert.assertEquals(1200, winArray[2].getBeginTimestamp());
    Assert.assertEquals(1000, winArray[2].getDurationMillis());
    Assert.assertEquals(1400, winArray[3].getBeginTimestamp());
    Assert.assertEquals(1000, winArray[3].getDurationMillis());
    Assert.assertEquals(1600, winArray[4].getBeginTimestamp());
    Assert.assertEquals(1000, winArray[4].getDurationMillis());
  }

  @Test
  public void testSessionWindowAssignment()
  {
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(1,
        new Attribute.AttributeMap.DefaultAttributeMap());
    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> windowedOperator = createDefaultKeyedWindowedOperator();
    windowedOperator.setWindowOption(new WindowOption.SessionWindows(Duration.millis(2000)));
    windowedOperator.setup(context);
    windowedOperator.beginWindow(1);
    Tuple tuple = new Tuple.TimestampedTuple<>(1100L, new KeyValPair<>("a", 2L));
    Tuple.WindowedTuple<KeyValPair<String, Long>> windowedValue = windowedOperator.getWindowedValue(tuple);
    List<Window> windows = windowedValue.getWindows();
    Assert.assertEquals(1, windows.size());
    Window.SessionWindow<String> sw = (Window.SessionWindow<String>)windows.get(0);
    Assert.assertEquals(1100L, sw.getBeginTimestamp());
    Assert.assertEquals(1, sw.getDurationMillis());
    windowedOperator.processTuple(tuple);

    // extending an existing session window
    tuple = new Tuple.TimestampedTuple<>(2000L, new KeyValPair<>("a", 3L));
    windowedValue = windowedOperator.getWindowedValue(tuple);
    windows = windowedValue.getWindows();
    sw = (Window.SessionWindow<String>)windows.get(0);
    Assert.assertEquals(1100L, sw.getBeginTimestamp());
    Assert.assertEquals(901, sw.getDurationMillis());
    windowedOperator.processTuple(tuple);

    // a separate session window
    tuple = new Tuple.TimestampedTuple<>(5000L, new KeyValPair<>("a", 4L));
    windowedValue = windowedOperator.getWindowedValue(tuple);
    windows = windowedValue.getWindows();
    sw = (Window.SessionWindow<String>)windows.get(0);
    Assert.assertEquals(5000L, sw.getBeginTimestamp());
    Assert.assertEquals(1, sw.getDurationMillis());
    windowedOperator.processTuple(tuple);

    // session window merging
    tuple = new Tuple.TimestampedTuple<>(3500L, new KeyValPair<>("a", 3L));
    windowedValue = windowedOperator.getWindowedValue(tuple);
    windows = windowedValue.getWindows();
    sw = (Window.SessionWindow<String>)windows.get(0);
    Assert.assertEquals(1100L, sw.getBeginTimestamp());
    Assert.assertEquals(3901, sw.getDurationMillis());
    windowedOperator.processTuple(tuple);

    windowedOperator.endWindow();
  }

  @Test
  public void testKeyedAccumulation()
  {
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(1,
        new Attribute.AttributeMap.DefaultAttributeMap());
    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> windowedOperator = createDefaultKeyedWindowedOperator();
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1000)));
    WindowedKeyedStorage<String, MutableLong> dataStorage = new InMemoryWindowedKeyedStorage<>();
    windowedOperator.setDataStorage(dataStorage);
    windowedOperator.setup(context);
    windowedOperator.beginWindow(1);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(100L, new KeyValPair<>("a", 2L)));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(200L, new KeyValPair<>("a", 3L)));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(300L, new KeyValPair<>("b", 4L)));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(150L, new KeyValPair<>("b", 5L)));
    windowedOperator.endWindow();
    Assert.assertEquals(1, dataStorage.size());
    Assert.assertEquals(5L, dataStorage.get(new Window.TimeWindow(0, 1000), "a").longValue());
    Assert.assertEquals(9L, dataStorage.get(new Window.TimeWindow(0, 1000), "b").longValue());
  }

  private void testKeyedTrigger(TriggerOption.AccumulationMode accumulationMode)
  {
    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> windowedOperator = createDefaultKeyedWindowedOperator();
    TriggerOption triggerOption = new TriggerOption().withEarlyFiringsAtEvery(Duration.millis(1000));
    switch (accumulationMode) {
      case ACCUMULATING:
        triggerOption.accumulatingFiredPanes();
        break;
      case ACCUMULATING_AND_RETRACTING:
        triggerOption.accumulatingAndRetractingFiredPanes();
        break;
      case DISCARDING:
        triggerOption.discardingFiredPanes();
        break;
      default:
        throw new RuntimeException("Unknown accumulation mode: " + accumulationMode);
    }
    windowedOperator.setTriggerOption(triggerOption);
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1000)));
    CollectorTestSink<Tuple<KeyValPair<String, Long>>> sink = new CollectorTestSink();
    windowedOperator.output.setSink((Sink<Object>)(Sink)sink);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(1,
        new Attribute.AttributeMap.DefaultAttributeMap());
    windowedOperator.setup(context);
    windowedOperator.beginWindow(1);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(100L, new KeyValPair<>("a", 2L)));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(200L, new KeyValPair<>("b", 3L)));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(400L, new KeyValPair<>("b", 5L)));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(300L, new KeyValPair<>("a", 4L)));
    windowedOperator.endWindow();
    Assert.assertTrue("No trigger should be fired yet", sink.collectedTuples.isEmpty());
    windowedOperator.beginWindow(2);
    windowedOperator.endWindow();
    Assert.assertTrue("No trigger should be fired yet", sink.collectedTuples.isEmpty());
    windowedOperator.beginWindow(3);
    windowedOperator.endWindow();
    Assert.assertEquals("There should be exactly two tuple for the time trigger", 2, sink.collectedTuples.size());
    {
      Map<String, Long> map = new HashMap<>();
      for (Tuple<KeyValPair<String, Long>> tuple : sink.collectedTuples) {
        map.put(tuple.getValue().getKey(), tuple.getValue().getValue());
      }
      Assert.assertEquals(6L, map.get("a").longValue());
      Assert.assertEquals(8L, map.get("b").longValue());
    }
    sink.collectedTuples.clear();
    windowedOperator.beginWindow(4);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(400L, new KeyValPair<>("a", 8L)));
    windowedOperator.endWindow();
    Assert.assertTrue("No trigger should be fired yet", sink.collectedTuples.isEmpty());
    windowedOperator.beginWindow(5);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(300L, new KeyValPair<>("b", 9L)));
    windowedOperator.endWindow();
    Map<String, Long> map = new HashMap<>();
    switch (accumulationMode) {
      case ACCUMULATING:
        Assert.assertEquals("There should be exactly two tuples for the time trigger", 2, sink.collectedTuples.size());
        for (Tuple<KeyValPair<String, Long>> tuple : sink.collectedTuples) {
          map.put(tuple.getValue().getKey(), tuple.getValue().getValue());
        }
        Assert.assertEquals(14L, map.get("a").longValue());
        Assert.assertEquals(17L, map.get("b").longValue());
        break;
      case DISCARDING:
        Assert.assertEquals("There should be exactly two tuples for the time trigger", 2, sink.collectedTuples.size());
        for (Tuple<KeyValPair<String, Long>> tuple : sink.collectedTuples) {
          map.put(tuple.getValue().getKey(), tuple.getValue().getValue());
        }
        Assert.assertEquals(8L, map.get("a").longValue());
        Assert.assertEquals(9L, map.get("b").longValue());
        break;
      case ACCUMULATING_AND_RETRACTING:
        Assert.assertEquals("There should be exactly four tuples for the time trigger", 4, sink.collectedTuples.size());
        for (Tuple<KeyValPair<String, Long>> tuple : sink.collectedTuples) {
          String key = tuple.getValue().getKey();
          long value = tuple.getValue().getValue();
          map.put(value < 0 ? "R" + key : key, value);
        }
        Assert.assertEquals(-6L, map.get("Ra").longValue());
        Assert.assertEquals(-8L, map.get("Rb").longValue());
        Assert.assertEquals(14L, map.get("a").longValue());
        Assert.assertEquals(17L, map.get("b").longValue());
        break;
      default:
        throw new RuntimeException("Unknown accumulation mode: " + accumulationMode);
    }
  }

  @Test
  public void testKeyedTriggerWithDiscardingMode()
  {
    testKeyedTrigger(TriggerOption.AccumulationMode.DISCARDING);
  }

  @Test
  public void testKeyedTriggerWithAccumulatingMode()
  {
    testKeyedTrigger(TriggerOption.AccumulationMode.ACCUMULATING);
  }

  @Test
  public void testKeyedTriggerWithAccumulatingAndRetractingMode()
  {
    testKeyedTrigger(TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING);
  }
}
