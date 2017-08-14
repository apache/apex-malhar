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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.validation.ValidationException;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponentImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableTestUtils;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.impl.FixedDiffEventTimeWatermarkGen;
import org.apache.apex.malhar.lib.window.impl.InMemorySessionWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.apex.malhar.lib.window.impl.SpillableSessionWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.SpillableWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.SpillableWindowedPlainStorage;
import org.apache.apex.malhar.lib.window.impl.WatermarkImpl;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;
import org.apache.commons.lang3.mutable.MutableLong;

import com.datatorrent.api.Sink;

/**
 * Unit tests for WindowedOperator
 */
@RunWith(Parameterized.class)
public class WindowedOperatorTest
{

  // To test the extreme condition counting from
  public static final long BASE = ((System.currentTimeMillis() - 3600000L * 24 * 365) / 1000) * 1000;

  @Parameterized.Parameters
  public static Collection<Object[]> testParameters()
  {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  @Parameterized.Parameter
  public Boolean useSpillable;

  private WindowedStorage.WindowedPlainStorage<WindowState> windowStateStorage;
  private WindowedStorage.WindowedPlainStorage<MutableLong> plainDataStorage;
  private WindowedStorage.WindowedPlainStorage<Long> plainRetractionStorage;
  private WindowedStorage.WindowedKeyedStorage<String, MutableLong> keyedDataStorage;
  private WindowedStorage.WindowedKeyedStorage<String, Long> keyedRetractionStorage;
  private SpillableComplexComponentImpl sccImpl;

  @Rule
  public SpillableTestUtils.TestMeta testMeta = new SpillableTestUtils.TestMeta();

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
    if (useSpillable) {
      sccImpl = new SpillableComplexComponentImpl(testMeta.timeStore);
      // TODO: We don't yet support Spillable data structures for window state storage because SpillableMapImpl does not yet support iterating over all keys.
      windowStateStorage = new InMemoryWindowedStorage<>();
      SpillableWindowedPlainStorage<MutableLong> pds = new SpillableWindowedPlainStorage<>();
      pds.setSpillableComplexComponent(sccImpl);
      plainDataStorage = pds;
      SpillableWindowedPlainStorage<Long> prs = new SpillableWindowedPlainStorage<>();
      prs.setSpillableComplexComponent(sccImpl);
      plainRetractionStorage = prs;
      windowedOperator.addComponent("SpillableComplexComponent", sccImpl);
    } else {
      windowStateStorage = new InMemoryWindowedStorage<>();
      plainDataStorage = new InMemoryWindowedStorage<>();
      plainRetractionStorage = new InMemoryWindowedStorage<>();
    }
    windowedOperator.setDataStorage(plainDataStorage);
    windowedOperator.setRetractionStorage(plainRetractionStorage);
    windowedOperator.setWindowStateStorage(windowStateStorage);
    windowedOperator.setAccumulation(new SumAccumulation());
    return windowedOperator;
  }

  private KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> createDefaultKeyedWindowedOperator(boolean forSession)
  {
    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> windowedOperator = new KeyedWindowedOperatorImpl<>();
    if (useSpillable) {
      sccImpl = new SpillableComplexComponentImpl(testMeta.timeStore);
      // TODO: We don't yet support Spillable data structures for window state storage because SpillableMapImpl does not yet support iterating over all keys.
      windowStateStorage = new InMemoryWindowedStorage<>();
      if (forSession) {
        SpillableSessionWindowedStorage<String, MutableLong> sws = new SpillableSessionWindowedStorage<>();
        sws.setSpillableComplexComponent(sccImpl);
        keyedDataStorage = sws;
      } else {
        SpillableWindowedKeyedStorage<String, MutableLong> kds = new SpillableWindowedKeyedStorage<>();
        kds.setSpillableComplexComponent(sccImpl);
        keyedDataStorage = kds;
      }
      SpillableWindowedKeyedStorage<String, Long> krs = new SpillableWindowedKeyedStorage<>();
      krs.setSpillableComplexComponent(sccImpl);
      keyedRetractionStorage = krs;
      windowedOperator.addComponent("SpillableComplexComponent", sccImpl);
    } else {
      windowStateStorage = new InMemoryWindowedStorage<>();
      if (forSession) {
        keyedDataStorage = new InMemorySessionWindowedStorage<>();
      } else {
        keyedDataStorage = new InMemoryWindowedKeyedStorage<>();
      }
      keyedRetractionStorage = new InMemoryWindowedKeyedStorage<>();
    }
    windowedOperator.setDataStorage(keyedDataStorage);
    windowedOperator.setRetractionStorage(keyedRetractionStorage);
    windowedOperator.setWindowStateStorage(windowStateStorage);
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
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createDefaultWindowedOperator();
    CollectorTestSink controlSink = new CollectorTestSink();

    windowedOperator.controlOutput.setSink(controlSink);

    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1000)));
    windowedOperator.setAllowedLateness(Duration.millis(1000));

    windowedOperator.setup(testMeta.operatorContext);
    windowedOperator.beginWindow(1);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 100L, 2L));
    Assert.assertEquals("There should be exactly one window in the storage", 1, plainDataStorage.size());
    Assert.assertEquals("There should be exactly one window in the storage", 1, windowStateStorage.size());

    Map.Entry<Window, WindowState> entry = windowStateStorage.entries().iterator().next();
    Window window = entry.getKey();
    WindowState windowState = entry.getValue();
    Assert.assertEquals(-1, windowState.watermarkArrivalTime);
    Assert.assertEquals(2L, plainDataStorage.get(window).longValue());
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 200L, 3L));
    Assert.assertEquals(5L, plainDataStorage.get(window).longValue());

    windowedOperator.processWatermark(new WatermarkImpl(BASE + 1200));
    windowedOperator.endWindow();
    Assert.assertTrue(windowState.watermarkArrivalTime >= 0);
    Assert.assertEquals("We should get one watermark tuple", 1, controlSink.getCount(false));

    windowedOperator.beginWindow(2);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 900L, 4L));
    Assert.assertEquals("Late but not too late", 9L, plainDataStorage.get(window).longValue());
    windowedOperator.processWatermark(new WatermarkImpl(BASE + 3000));
    windowedOperator.endWindow();
    Assert.assertEquals("We should get two watermark tuples", 2, controlSink.getCount(false));
    windowedOperator.beginWindow(3);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 120L, 5L)); // this tuple should be dropped
    Assert.assertEquals("The window should be dropped because it's too late", 0, plainDataStorage.size());
    Assert.assertEquals("The window should be dropped because it's too late", 0, windowStateStorage.size());
    windowedOperator.endWindow();
    windowedOperator.teardown();
  }

  @Test
  public void testImplicitWatermarks()
  {
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createDefaultWindowedOperator();
    CollectorTestSink controlSink = new CollectorTestSink();

    windowedOperator.controlOutput.setSink(controlSink);

    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1000)));
    windowedOperator.setAllowedLateness(Duration.millis(1000));
    windowedOperator.setImplicitWatermarkGenerator(new FixedDiffEventTimeWatermarkGen(100));

    windowedOperator.setup(testMeta.operatorContext);

    windowedOperator.beginWindow(1);
    windowedOperator.endWindow();
    Assert.assertEquals("We should get no watermark tuple", 0, controlSink.getCount(false));

    windowedOperator.beginWindow(2);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 100L, 2L));
    windowedOperator.endWindow();
    Assert.assertEquals("We should get one watermark tuple", 1, controlSink.getCount(false));
    Assert.assertEquals("Check Watermark value",
        ((ControlTuple.Watermark)controlSink.collectedTuples.get(0)).getTimestamp(), BASE);

    windowedOperator.beginWindow(3);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 900L, 4L));
    windowedOperator.endWindow();
    Assert.assertEquals("We should get two watermark tuples", 2, controlSink.getCount(false));
    Assert.assertEquals("Check Watermark value",
        ((ControlTuple.Watermark)controlSink.collectedTuples.get(1)).getTimestamp(), BASE + 800);
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
    windowedOperator.setup(testMeta.operatorContext);
    windowedOperator.beginWindow(1);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 100L, 2L));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 200L, 3L));
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
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 400L, 4L));
    windowedOperator.endWindow();
    Assert.assertTrue("No trigger should be fired yet", sink.collectedTuples.isEmpty());
    windowedOperator.beginWindow(5);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 300L, 5L));
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
    windowedOperator.teardown();
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
  public void testTriggerWithAccumulatingModeFiringAllPanes()
  {
    testTrigger2(false, false);
  }

  @Test
  public void testTriggerWithAccumulatingAndRetractingModeFiringAllPanes()
  {
    testTrigger2(false, true);
  }

  @Test
  public void testTriggerWithAccumulatingModeFiringOnlyUpdatedPanes()
  {
    testTrigger2(true, false);
  }

  @Test
  public void testTriggerWithAccumulatingAndRetractingModeFiringOnlyUpdatedPanes()
  {
    testTrigger2(true, true);
  }

  private void testTrigger2(boolean firingOnlyUpdatedPanes, boolean testRetraction)
  {
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createDefaultWindowedOperator();
    TriggerOption triggerOption = new TriggerOption().withEarlyFiringsAtEvery(Duration.millis(1000));
    if (testRetraction) {
      triggerOption.accumulatingAndRetractingFiredPanes();
    } else {
      triggerOption.accumulatingFiredPanes();
    }
    if (firingOnlyUpdatedPanes) {
      triggerOption.firingOnlyUpdatedPanes();
    }
    windowedOperator.setTriggerOption(triggerOption);
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1000)));
    CollectorTestSink sink = new CollectorTestSink();
    windowedOperator.output.setSink(sink);
    windowedOperator.setup(testMeta.operatorContext);
    windowedOperator.beginWindow(1);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 100L, 2L));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 200L, 3L));
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
      Assert.assertTrue("There should not be any trigger since no panes have been updated", sink.collectedTuples
          .isEmpty());
    } else {
      if (testRetraction) {
        Assert.assertEquals("There should be exactly two tuples for the time trigger", 2, sink.collectedTuples.size());
        Assert.assertEquals(-5L, ((Tuple<Long>)sink.collectedTuples.get(0)).getValue().longValue());
        Assert.assertEquals(5L, ((Tuple<Long>)sink.collectedTuples.get(1)).getValue().longValue());
      } else {
        Assert.assertEquals("There should be exactly one tuple for the time trigger", 1, sink.collectedTuples.size());
        Assert.assertEquals(5L, ((Tuple<Long>)sink.collectedTuples.get(0)).getValue().longValue());
      }
    }
    windowedOperator.teardown();
  }

  @Test
  public void testGlobalWindowAssignment()
  {
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createDefaultWindowedOperator();
    windowedOperator.setWindowOption(new WindowOption.GlobalWindow());
    windowedOperator.setup(testMeta.operatorContext);
    Tuple.WindowedTuple<Long> windowedValue = windowedOperator.getWindowedValue(new Tuple.TimestampedTuple<>(BASE + 1100L, 2L));
    Collection<? extends Window> windows = windowedValue.getWindows();
    Assert.assertEquals(1, windows.size());
    Assert.assertEquals(Window.GlobalWindow.INSTANCE, windows.iterator().next());
    windowedOperator.teardown();
  }

  @Test
  public void testTimeWindowAssignment()
  {
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createDefaultWindowedOperator();
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1000)));
    windowedOperator.setup(testMeta.operatorContext);
    Tuple.WindowedTuple<Long> windowedValue = windowedOperator.getWindowedValue(new Tuple.TimestampedTuple<>(BASE + 1100L, 2L));
    Collection<? extends Window> windows = windowedValue.getWindows();
    Assert.assertEquals(1, windows.size());
    Window window = windows.iterator().next();
    Assert.assertEquals(BASE + 1000, window.getBeginTimestamp());
    Assert.assertEquals(1000, window.getDurationMillis());
  }

  @Test
  public void testSlidingWindowAssignment()
  {
    WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createDefaultWindowedOperator();
    windowedOperator.setWindowOption(new WindowOption.SlidingTimeWindows(Duration.millis(1000), Duration.millis(200)));
    windowedOperator.setup(testMeta.operatorContext);
    Tuple.WindowedTuple<Long> windowedValue = windowedOperator.getWindowedValue(new Tuple.TimestampedTuple<>(BASE + 1600L, 2L));
    Collection<? extends Window> windows = windowedValue.getWindows();
    Window[] winArray = windows.toArray(new Window[]{});
    Assert.assertEquals(5, winArray.length);
    Assert.assertEquals(BASE + 800, winArray[0].getBeginTimestamp());
    Assert.assertEquals(1000, winArray[0].getDurationMillis());
    Assert.assertEquals(BASE + 1000, winArray[1].getBeginTimestamp());
    Assert.assertEquals(1000, winArray[1].getDurationMillis());
    Assert.assertEquals(BASE + 1200, winArray[2].getBeginTimestamp());
    Assert.assertEquals(1000, winArray[2].getDurationMillis());
    Assert.assertEquals(BASE + 1400, winArray[3].getBeginTimestamp());
    Assert.assertEquals(1000, winArray[3].getDurationMillis());
    Assert.assertEquals(BASE + 1600, winArray[4].getBeginTimestamp());
    Assert.assertEquals(1000, winArray[4].getDurationMillis());
    windowedOperator.teardown();
  }

  @Test
  public void testSessionWindows()
  {
    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> windowedOperator = createDefaultKeyedWindowedOperator(true);
    windowedOperator.setWindowOption(new WindowOption.SessionWindows(Duration.millis(2000)));
    windowedOperator.setTriggerOption(new TriggerOption().withEarlyFiringsAtEvery(1).accumulatingAndRetractingFiredPanes().firingOnlyUpdatedPanes());
    CollectorTestSink<Tuple<KeyValPair<String, Long>>> sink = new CollectorTestSink();
    windowedOperator.output.setSink((Sink<Object>)(Sink)sink);
    windowedOperator.setup(testMeta.operatorContext);
    windowedOperator.beginWindow(1);
    Tuple<KeyValPair<String, Long>> tuple = new Tuple.TimestampedTuple<>(BASE + 1100L, new KeyValPair<>("a", 2L));
    windowedOperator.processTuple(tuple);

    Assert.assertEquals(1, sink.getCount(false));
    Tuple.WindowedTuple<KeyValPair<String, Long>> out = (Tuple.WindowedTuple<KeyValPair<String, Long>>)sink.collectedTuples.get(0);
    Assert.assertEquals(1, out.getWindows().size());
    Window.SessionWindow<String> window1 = (Window.SessionWindow<String>)out.getWindows().iterator().next();
    Assert.assertEquals(BASE + 1100L, window1.getBeginTimestamp());
    Assert.assertEquals(2000, window1.getDurationMillis());
    Assert.assertEquals("a", window1.getKey());
    Assert.assertEquals("a", out.getValue().getKey());
    Assert.assertEquals(2L, out.getValue().getValue().longValue());
    sink.clear();

    // extending an existing session window
    tuple = new Tuple.TimestampedTuple<>(BASE + 2000L, new KeyValPair<>("a", 3L));
    windowedOperator.processTuple(tuple);
    Assert.assertEquals(2, sink.getCount(false));

    // retraction trigger
    out = (Tuple.WindowedTuple<KeyValPair<String, Long>>)sink.collectedTuples.get(0);
    Assert.assertEquals(1, out.getWindows().size());
    Assert.assertEquals(window1, out.getWindows().iterator().next());
    Assert.assertEquals("a", out.getValue().getKey());
    Assert.assertEquals(-2L, out.getValue().getValue().longValue());

    // normal trigger
    out = (Tuple.WindowedTuple<KeyValPair<String, Long>>)sink.collectedTuples.get(1);
    Window.SessionWindow<String> window2 = (Window.SessionWindow<String>)out.getWindows().iterator().next();

    Assert.assertEquals(BASE + 1100L, window2.getBeginTimestamp());
    Assert.assertEquals(2900, window2.getDurationMillis());
    Assert.assertEquals("a", out.getValue().getKey());
    Assert.assertEquals(5L, out.getValue().getValue().longValue());
    sink.clear();

    // a separate session window
    tuple = new Tuple.TimestampedTuple<>(BASE + 5000L, new KeyValPair<>("a", 4L));
    windowedOperator.processTuple(tuple);
    Assert.assertEquals(1, sink.getCount(false));
    out = (Tuple.WindowedTuple<KeyValPair<String, Long>>)sink.collectedTuples.get(0);
    Assert.assertEquals(1, out.getWindows().size());
    Window.SessionWindow<String> window3 = (Window.SessionWindow<String>)out.getWindows().iterator().next();
    Assert.assertEquals(BASE + 5000L, window3.getBeginTimestamp());
    Assert.assertEquals(2000, window3.getDurationMillis());
    Assert.assertEquals("a", out.getValue().getKey());
    Assert.assertEquals(4L, out.getValue().getValue().longValue());
    sink.clear();

    // session window merging
    tuple = new Tuple.TimestampedTuple<>(BASE + 3500L, new KeyValPair<>("a", 3L));
    windowedOperator.processTuple(tuple);

    Assert.assertEquals(3, sink.getCount(false));

    // retraction of the two old windows
    Map<Window, KeyValPair<String, Long>> tuples = new TreeMap<>();
    out = (Tuple.WindowedTuple<KeyValPair<String, Long>>)sink.collectedTuples.get(0);
    Assert.assertEquals(1, out.getWindows().size());
    tuples.put(out.getWindows().iterator().next(), out.getValue());

    out = (Tuple.WindowedTuple<KeyValPair<String, Long>>)sink.collectedTuples.get(1);
    Assert.assertEquals(1, out.getWindows().size());
    tuples.put(out.getWindows().iterator().next(), out.getValue());

    Iterator<Map.Entry<Window, KeyValPair<String, Long>>> iterator = tuples.entrySet().iterator();
    Map.Entry<Window, KeyValPair<String, Long>> entry = iterator.next();
    Assert.assertEquals(window2, entry.getKey());
    Assert.assertEquals("a", entry.getValue().getKey());
    Assert.assertEquals(-5L, entry.getValue().getValue().longValue());
    entry = iterator.next();
    Assert.assertEquals(window3, entry.getKey());
    Assert.assertEquals("a", entry.getValue().getKey());
    Assert.assertEquals(-4L, entry.getValue().getValue().longValue());

    // normal trigger
    out = (Tuple.WindowedTuple<KeyValPair<String, Long>>)sink.collectedTuples.get(2);
    Assert.assertEquals(1, out.getWindows().size());
    Window.SessionWindow<String> window4 = (Window.SessionWindow<String>)out.getWindows().iterator().next();
    Assert.assertEquals(BASE + 1100L, window4.getBeginTimestamp());
    Assert.assertEquals(5900, window4.getDurationMillis());
    Assert.assertEquals("a", out.getValue().getKey());
    Assert.assertEquals(12L, out.getValue().getValue().longValue());

    windowedOperator.endWindow();
    windowedOperator.teardown();
  }

  @Test
  public void testKeyedAccumulation()
  {
    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> windowedOperator = createDefaultKeyedWindowedOperator(false);
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1000)));
    windowedOperator.setup(testMeta.operatorContext);
    windowedOperator.beginWindow(1);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 100L, new KeyValPair<>("a", 2L)));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 200L, new KeyValPair<>("a", 3L)));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 300L, new KeyValPair<>("b", 4L)));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 150L, new KeyValPair<>("b", 5L)));
    windowedOperator.endWindow();
    Assert.assertEquals(1, keyedDataStorage.size());
    Assert.assertEquals(5L, keyedDataStorage.get(new Window.TimeWindow(BASE, 1000), "a").longValue());
    Assert.assertEquals(9L, keyedDataStorage.get(new Window.TimeWindow(BASE, 1000), "b").longValue());
    windowedOperator.teardown();
  }

  private void testKeyedTrigger(TriggerOption.AccumulationMode accumulationMode)
  {
    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> windowedOperator = createDefaultKeyedWindowedOperator(false);
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
    windowedOperator.setup(testMeta.operatorContext);
    windowedOperator.beginWindow(1);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 100L, new KeyValPair<>("a", 2L)));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 200L, new KeyValPair<>("b", 3L)));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 400L, new KeyValPair<>("b", 5L)));
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 300L, new KeyValPair<>("a", 4L)));
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
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 400L, new KeyValPair<>("a", 8L)));
    windowedOperator.endWindow();
    Assert.assertTrue("No trigger should be fired yet", sink.collectedTuples.isEmpty());
    windowedOperator.beginWindow(5);
    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(BASE + 300L, new KeyValPair<>("b", 9L)));
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
    windowedOperator.teardown();
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
