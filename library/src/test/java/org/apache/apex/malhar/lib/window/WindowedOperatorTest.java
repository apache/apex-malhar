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

import java.util.Map;

import javax.validation.ValidationException;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.WatermarkImpl;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;

import com.datatorrent.api.Attribute;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

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

  private WindowedOperatorImpl<Long, Long, Long> createDefaultWindowedOperator()
  {
    WindowedOperatorImpl<Long, Long, Long> windowedOperator = new WindowedOperatorImpl<>();
    windowedOperator.setDataStorage(new InMemoryWindowedStorage<Long>());
    windowedOperator.setRetractionStorage(new InMemoryWindowedStorage<Long>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowedOperator.setDataStorage(new InMemoryWindowedStorage<Long>());
    windowedOperator.setAccumulation(new SumAccumulation());
    return windowedOperator;
  }

  @Test
  public void testValidation() throws Exception
  {
    WindowedOperatorImpl<Long, Long, Long> windowedOperator = new WindowedOperatorImpl<>();
    verifyValidationFailure(windowedOperator, "nothing is configured");
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    verifyValidationFailure(windowedOperator, "data storage is not set");
    windowedOperator.setDataStorage(new InMemoryWindowedStorage<Long>());
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
    WindowedOperatorImpl<Long, Long, Long> windowedOperator = createDefaultWindowedOperator();
    CollectorTestSink controlSink = new CollectorTestSink();

    windowedOperator.controlOutput.setSink(controlSink);

    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1000)));
    windowedOperator.setAllowedLateness(Duration.millis(1000));

    WindowedStorage<Long> dataStorage = new InMemoryWindowedStorage<>();
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
    Assert.assertTrue(windowState.watermarkArrivalTime > 0);
    Assert.assertEquals("We should get one watermark tuple", 1, controlSink.getCount(false));

    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(900L, 4L));
    Assert.assertEquals("Late but not too late", 9L, dataStorage.get(window).longValue());

    windowedOperator.endWindow();

    windowedOperator.beginWindow(2);
    windowedOperator.processWatermark(new WatermarkImpl(3000));
    Assert.assertEquals("We should get two watermark tuples", 2, controlSink.getCount(false));

    windowedOperator.processTuple(new Tuple.TimestampedTuple<>(120L, 5L)); // this tuple should be dropped
    Assert.assertEquals("The window should be dropped because it's too late", 0, dataStorage.size());
    Assert.assertEquals("The window should be dropped because it's too late", 0, windowStateStorage.size());
    windowedOperator.endWindow();
  }

  @Test
  public void testTriggerWithDiscardingMode()
  {

  }

  @Test
  public void testTriggerWithAccumulatingMode()
  {

  }

  @Test
  public void testTriggerWithAccumulatingModeFiringOnlyUpdatedPanes()
  {

  }

  @Test
  public void testTriggerWithAccumulatingAndRetractingMode()
  {

  }

  @Test
  public void testGlobalWindow()
  {

  }

  @Test
  public void testTimeWindow()
  {

  }

  @Test
  public void testSlidingWindow()
  {

  }

  @Test
  public void testSessionWindow()
  {

  }
}
