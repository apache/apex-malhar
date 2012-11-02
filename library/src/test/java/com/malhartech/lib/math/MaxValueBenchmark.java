/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.dag.TestCountAndLastTupleSink;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.MaxValue}<p>
 *
 */
public class MaxValueBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MaxValueBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    MaxValue<Double> oper = new MaxValue<Double>();
    TestCountAndLastTupleSink maxSink = new TestCountAndLastTupleSink();

    oper.max.setSink(maxSink);

    // Not needed, but still setup is being called as a matter of discipline
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null));
    oper.beginWindow(0); //

    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
    Double a = new Double(2.0);
    Double b = new Double(20.0);
    Double c = new Double(1000.0);
    oper.data.process(a);
    oper.data.process(b);
    oper.data.process(c);
    a = 1.0;
    oper.data.process(a);
    a = 10.0;
    oper.data.process(a);
    b = 5.0;
    oper.data.process(b);
    b = 12.0;
    oper.data.process(b);
    c = 22.0;
    oper.data.process(c);
    c = 14.0;
    oper.data.process(c);
    a = 46.0;
    oper.data.process(a);
    b = 2.0;
    oper.data.process(b);
    a = 23.0;
    oper.data.process(a);
    }

    oper.endWindow(); //
    log.debug(String.format("\nBenchmark for %d tuples; expected 1.0, got %f from %d tuples", numTuples*12,
                            (Double) maxSink.tuple, maxSink.count));
  }
}
