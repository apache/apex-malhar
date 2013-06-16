/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
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