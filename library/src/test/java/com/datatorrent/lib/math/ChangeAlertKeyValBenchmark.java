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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.ChangeAlertKeyVal;
import com.datatorrent.lib.testbench.CountTestSink;
import com.datatorrent.lib.util.KeyValPair;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.ChangeAlertKeyVal}. <p>
 * Current benchmark is 33 millions tuples per second.
 *
 */
public class ChangeAlertKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ChangeAlertKeyValBenchmark.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Integer>());
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Double>());
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Float>());
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Short>());
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Long>());
  }

  public <V extends Number> void testNodeProcessingSchema(ChangeAlertKeyVal<String, V> oper)
  {
    CountTestSink alertSink = new CountTestSink<KeyValPair<String, KeyValPair<V, Double>>>();

    oper.alert.setSink(alertSink);
    oper.setPercentThreshold(5);

    oper.beginWindow(0);

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(new KeyValPair<String, V>("a", oper.getValue(200)));
      oper.data.process(new KeyValPair<String, V>("b", oper.getValue(10)));
      oper.data.process(new KeyValPair<String, V>("c", oper.getValue(100)));
    }
    oper.endWindow();

    log.debug(String.format("\nBenchmarked %d tuples, emitted %d", numTuples * 3, alertSink.getCount()));
  }
}
