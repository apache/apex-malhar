/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestSink;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.Sum}. <p>
 *
 */
public class SumBenchmark
{
  private static Logger log = LoggerFactory.getLogger(SumBenchmark.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    Sum<Double> doper = new Sum<Double>();
    Sum<Float> foper = new Sum<Float>();
    Sum<Integer> ioper = new Sum<Integer>();
    Sum<Long> loper = new Sum<Long>();
    Sum<Short> soper = new Sum<Short>();
    doper.setType(Double.class);
    foper.setType(Float.class);
    ioper.setType(Integer.class);
    loper.setType(Long.class);
    soper.setType(Short.class);

    testNodeSchemaProcessing(doper, "Double");
    testNodeSchemaProcessing(foper, "Float");
    testNodeSchemaProcessing(ioper, "Integer");
    testNodeSchemaProcessing(loper, "Long");
    testNodeSchemaProcessing(soper, "Short");
  }

  public void testNodeSchemaProcessing(Sum oper, String debug)
  {
    TestSink sumSink = new TestSink();
    oper.sum.setSink(sumSink);

    oper.beginWindow(0); //

    Double a = new Double(2.0);
    Double b = new Double(2.0);
    Double c = new Double(1.0);

    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(a);
      oper.data.process(b);
      oper.data.process(c);
    }
    oper.endWindow(); //

    Number dval = (Number)sumSink.collectedTuples.get(0);
    log.debug(String.format("\nBenchmark %d tuples of type %s: total was %f",
                            numTuples * 3, debug, dval.doubleValue()));
  }
}
