/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.api.Sink;
import com.datatorrent.lib.math.Quotient;
import com.datatorrent.tuple.Tuple;
import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.Quotient}<p>
 *
 */
public class QuotientBenchmark
{
  private static Logger log = LoggerFactory.getLogger(QuotientBenchmark.class);

  class TestSink implements Sink
  {
    ArrayList<Object> collectedTuples = new ArrayList<Object>();

    @Override
    public void put(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        collectedTuples.add(payload);
      }
    }

    @Override
    public int getCount(boolean reset)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing()
  {
    Quotient<Double> oper = new Quotient<Double>();
    TestSink quotientSink = new TestSink();
    oper.quotient.setSink(quotientSink);

    oper.setMult_by(2);

    int numTuples = 100000000;
    oper.beginWindow(0); //
    for (int i = 0; i < numTuples; i++) {
      Double a = new Double(30.0);
      Double b = new Double(20.0);
      Double c = new Double(100.0);
      oper.denominator.process(a);
      oper.denominator.process(b);
      oper.denominator.process(c);

      a = 5.0;
      oper.numerator.process(a);
      a = 1.0;
      oper.numerator.process(a);
      b = 44.0;
      oper.numerator.process(b);

      b = 10.0;
      oper.numerator.process(b);
      c = 22.0;
      oper.numerator.process(c);
      c = 18.0;
      oper.numerator.process(c);

      a = 0.5;
      oper.numerator.process(a);
      b = 41.5;
      oper.numerator.process(b);
      a = 8.0;
      oper.numerator.process(a);
    }
    oper.endWindow(); //

    // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
    Assert.assertEquals("number emitted tuples", 1, quotientSink.collectedTuples.size());
    Double val = (Double)quotientSink.collectedTuples.get(0);
    log.debug(String.format("\nBenchmark for %d tuples (expected 2.0, got %f)", numTuples * 12, val));
  }
}
