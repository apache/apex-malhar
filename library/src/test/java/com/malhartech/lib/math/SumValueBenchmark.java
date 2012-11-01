/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.SumValue}<p>
 *
 */
public class SumValueBenchmark {
    private static Logger log = LoggerFactory.getLogger(SumValueBenchmark.class);

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
    SumValue<Double> oper = new SumValue<Double>();
    oper.setType(Double.class);
    TestSink sumSink = new TestSink();
    TestSink countSink = new TestSink();
    oper.sum.setSink(sumSink);
    oper.count.setSink(countSink);

    // Not needed, but still setup is being called as a matter of discipline
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null));
    oper.beginWindow(); //

    Double a = new Double(2.0);
    Double b = new Double(2.0);
    Double c = new Double(1.0);

    int numTuples = 100000000;
    double val = 5.0;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(a);
      oper.data.process(b);
      oper.data.process(c);
      val += 5.0;
    }
    oper.endWindow(); //

    Double dval = (Double) sumSink.collectedTuples.get(0);
    Integer count = (Integer) countSink.collectedTuples.get(0);
    log.debug(String.format("\nBenchmark total was %f, expected %f; count was %d, expected %d tuples", dval, val, count, numTuples*3));
  }
}
