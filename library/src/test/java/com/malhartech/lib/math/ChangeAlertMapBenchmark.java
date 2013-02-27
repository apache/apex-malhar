/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.lib.testbench.TestCountSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.ChangeAlertMap}. <p>
 *
 */
public class ChangeAlertMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ChangeAlertMapBenchmark.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeAlertMap<String, Integer>());
    testNodeProcessingSchema(new ChangeAlertMap<String, Double>());
    testNodeProcessingSchema(new ChangeAlertMap<String, Float>());
    testNodeProcessingSchema(new ChangeAlertMap<String, Short>());
    testNodeProcessingSchema(new ChangeAlertMap<String, Long>());
  }

  public <V extends Number> void testNodeProcessingSchema(ChangeAlertMap<String, V> oper)
  {
    TestCountSink<HashMap<String, HashMap<V, Double>>> alertSink = new TestCountSink<HashMap<String, HashMap<V, Double>>>();

    oper.alert.setSink(alertSink);
    oper.setPercentThreshold(5);

    oper.beginWindow(0);
    HashMap<String, V> input = new HashMap<String, V>();

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.put("a", oper.getValue(i));
      input.put("b", oper.getValue(i + 2));
      oper.data.process(input);

      input.clear();
      input.put("a", oper.getValue(i + 1));
      input.put("b", oper.getValue(i + 3));

      oper.data.process(input);
      if (i % 100000 == 0) {
        input.clear();
        input.put("a", oper.getValue(10));
        input.put("b", oper.getValue(33));
        oper.data.process(input);
      }
    }
    oper.endWindow();
    // One for each key
    log.debug(String.format("\nBenchmarked %d tuples, emitted %d", numTuples * 4, alertSink.getCount()));
  }
}