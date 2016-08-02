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
package org.apache.apex.malhar.lib.dedup;

import java.util.Date;
import java.util.List;
import java.util.Random;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Tuple.TimestampedTuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.stram.engine.WindowGenerator;

public class WindowedDeduperTest
{
  private static final int OPERATOR_ID = 0;
  private static AbstractWindowedDeduper<TestPojo> dedup;
  private static final int WINDOW_SPAN = 10;
  private static final int FIXED_WATERMARK = 30;

  /**
   * Windowed Deduper Test Implementation
   * Tuples of type {@link TestPojo}
   */
  public static class WindowedDeduperImpl extends AbstractWindowedDeduper<TestPojo>
  {
    @Override
    protected Object getKey(TestPojo tuple)
    {
      return tuple.getKey();
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @BeforeClass
  public static void setup()
  {
    dedup = new WindowedDeduperImpl();
    // Define accumulation
    Accumulation dedupAccum = new DedupAccumulation<TestPojo>();
    dedup.setAccumulation(dedupAccum);
    // Set storage to InMemory implementation
    dedup.setDataStorage(new InMemoryWindowedStorage<List<TestPojo>>());
    // Set retraction storage. Not used, but is required by the abstract operator.
    dedup.setRetractionStorage(new InMemoryWindowedStorage<TestPojo>());
    // Set window state storage
    dedup.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    // 10 second wide time windows
    dedup.setWindowOption(new WindowOption.TimeWindows(Duration.standardSeconds(WINDOW_SPAN)));
    // Watermark at 30 seconds (30000 milliseconds) - This acts as the expiry
    dedup.setFixedWatermark(FIXED_WATERMARK * 1000);
    // Set allowed lateness to enable expiry - Dropping too late tuples
    dedup.setAllowedLateness(Duration.ZERO);
  }

  @Test
  public void testWindowedDedup() throws InterruptedException
  {
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes =
        new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    OperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes);

    dedup.setup(context);

    CollectorTestSink<TestPojo> uniqueSink = new CollectorTestSink<TestPojo>();
    TestUtils.setSink(dedup.unique, uniqueSink);
    CollectorTestSink<TestPojo> duplicateSink = new CollectorTestSink<TestPojo>();
    TestUtils.setSink(dedup.duplicates, duplicateSink);
    CollectorTestSink<TestPojo> expiredSink = new CollectorTestSink<TestPojo>();
    TestUtils.setSink(dedup.expired, expiredSink);

    // Time corresponding to the first window
    long firstWindowTimeInMillis = 1451590200000L; // JAN-1-2016 01:00:00 IST
    long currentWindow = -1;

    Tuple.TimestampedTuple<TestPojo> tuple;
    TestPojo pojo;
    Random r = new Random();
    List<Tuple<TestPojo>> allUniqueTuples = Lists.newArrayList();
    long sequence = 0;
    int uniqueCount = 0;
    int duplicateCount = 0;
    int expiredCount = 0;

    // Start Streaming Windows
    for (int i = 1; i <= 100; i++) {

      // Generate realistic window id for correct calculation of watermark
      // Simulate 500 ms wide streaming windows
      currentWindow = WindowGenerator.getNextWindowId(currentWindow, firstWindowTimeInMillis, 500);
      long currentWindowTime = firstWindowTimeInMillis + ((i - 1) * 500);

      dedup.beginWindow(currentWindow);

      // Send unique events
      long eventTime = currentWindowTime;
      pojo = new TestPojo(i, new Date(eventTime), sequence++, "UNIQUE");
      tuple = new TimestampedTuple<TestPojo>(eventTime, pojo);
      dedup.input.process(tuple);
      uniqueCount++;
      allUniqueTuples.add(tuple);

      // Send duplicates and Expired 50% of the time
      if (i % 2 == 0) {
        TimestampedTuple<TestPojo> t = (TimestampedTuple<TestPojo>)allUniqueTuples.get(r.nextInt(allUniqueTuples.size()));
        TestPojo p = new TestPojo(t.getValue().getKey(), t.getValue().getDate(), t.getValue().getSequence(),"");
        tuple = new TimestampedTuple<TestPojo>(t.getTimestamp(), p);

        if (p.getDate().getTime() < (currentWindowTime + 1) - (FIXED_WATERMARK * 1000) - 500) {
          p.setLabel("EXPIRED");
          expiredCount++;
        } else {
          p.setLabel("DUPLICATE");
          duplicateCount++;
        }
        dedup.input.process(tuple);
      }

      dedup.endWindow();
    }

    // Verify counts
    Assert.assertEquals("Unique Count Mismatch", uniqueSink.collectedTuples.size(), uniqueCount);
    Assert.assertEquals("Duplicate Count Mismatch", duplicateSink.collectedTuples.size(), duplicateCount);
    Assert.assertEquals("Expired Count Mismatch", expiredSink.collectedTuples.size(), expiredCount);

    dedup.teardown();
  }
}
