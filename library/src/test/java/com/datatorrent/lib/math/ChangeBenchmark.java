/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.lib.testbench.CountTestSink;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.Change}. <p>
 *
 */
public class ChangeBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ChangeBenchmark.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Change<Integer>());
    testNodeProcessingSchema(new Change<Double>());
    testNodeProcessingSchema(new Change<Float>());
    testNodeProcessingSchema(new Change<Short>());
    testNodeProcessingSchema(new Change<Long>());
  }

  /**
   *
   * @param oper
   */
  public <V extends Number> void testNodeProcessingSchema(Change<V> oper)
  {
    CountTestSink changeSink = new CountTestSink<V>();
    CountTestSink percentSink = new CountTestSink<Double>();

    oper.change.setSink(changeSink);
    oper.percent.setSink(percentSink);

    oper.beginWindow(0);
    int numTuples = 1000000;
    for (int i = 0; i < numTuples; i++) {
      oper.base.process(oper.getValue(10));
      oper.data.process(oper.getValue(5));
      oper.data.process(oper.getValue(15));
      oper.data.process(oper.getValue(20));
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", numTuples * 3, changeSink.getCount());
    Assert.assertEquals("number emitted tuples", numTuples * 3, percentSink.getCount());
    log.debug(String.format("\nBenchmarked %d key,val pairs", numTuples * 3));
  }
}