/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.lib.testbench.CountTestSink;
import com.malhartech.lib.util.KeyValPair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.ChangeAlert}. <p>
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
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
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