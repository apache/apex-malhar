/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.Quotient}<p>
 *
 */
public class QuotientValueTest
{
  private static Logger LOG = LoggerFactory.getLogger(SumMap.class);

  class TestSink implements Sink
  {
    List<Object> collectedTuples = new ArrayList<Object>();

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
  public void testNodeSchemaProcessing()
  {
    Quotient<Double> oper = new Quotient<Double>();
    TestSink quotientSink = new TestSink();
    oper.quotient.setSink(quotientSink);

    oper.setMult_by(2);

    oper.beginWindow(0); //
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
    oper.endWindow(); //

    // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
    Assert.assertEquals("number emitted tuples", 1, quotientSink.collectedTuples.size());
    for (Object o: quotientSink.collectedTuples) { // sum is 1157
      Double val = (Double)o;
      Assert.assertEquals("emitted quotient value was ", new Double(2.0), val);
    }
  }
}
