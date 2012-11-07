/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 *
 * Performance tests for {@link com.malhartech.lib.math.QuotientValue}<p>
 *
 */
public class QuotientValueBenchmark
{
  private static Logger log = LoggerFactory.getLogger(QuotientValueBenchmark.class);

  class TestSink implements Sink
  {
    ArrayList<Object> collectedTuples = new ArrayList<Object>();

    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        collectedTuples.add(payload);
      }
    }
  }

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing()
  {
    QuotientValue<Double> oper = new QuotientValue<Double>();
    TestSink quotientSink = new TestSink();
    oper.quotient.setSink(quotientSink);

    // Not needed, but still setup is being called as a matter of discipline
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null, null));
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
    Double val = (Double) quotientSink.collectedTuples.get(0);
    log.debug(String.format("\nBenchmark for %d tuples (expected 2.0, got %f)", numTuples*12, val));
  }
}
