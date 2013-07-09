/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.ChangeAlert;
import com.datatorrent.lib.testbench.CountTestSink;
import com.datatorrent.lib.util.KeyValPair;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.ChangeAlert}. <p>
 * Current benchmark is 94 millions tuples per second.
 *
 */
public class ChangeAlertBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ChangeAlertBenchmark.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeAlert<Integer>());
    testNodeProcessingSchema(new ChangeAlert<Double>());
    testNodeProcessingSchema(new ChangeAlert<Float>());
    testNodeProcessingSchema(new ChangeAlert<Short>());
    testNodeProcessingSchema(new ChangeAlert<Long>());
  }

  public <V extends Number> void testNodeProcessingSchema(ChangeAlert<V> oper)
  {
    CountTestSink alertSink = new CountTestSink<KeyValPair<V, Double>>();


    oper.alert.setSink(alertSink);
    oper.setPercentThreshold(5);

    oper.beginWindow(0);

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(oper.getValue(10));
      oper.data.process(oper.getValue(12)); // alert
      oper.data.process(oper.getValue(12));
      oper.data.process(oper.getValue(18)); // alert
      oper.data.process(oper.getValue(0));  // alert
      oper.data.process(oper.getValue(20)); // this will not alert
      oper.data.process(oper.getValue(30)); // alert
    }
    oper.endWindow();

    log.debug(String.format("\nBenchmarked %d tuples, emitted %d", numTuples * 7, alertSink.getCount()));
  }
}
