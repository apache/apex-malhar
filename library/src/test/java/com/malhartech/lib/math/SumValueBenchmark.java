/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;
import java.util.ArrayList;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.SumValue}<p>
 *
 */
public class SumValueBenchmark
{
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
  public void testNodeProcessing()
  {
    SumValue<Double> doper = new SumValue<Double>();
    SumValue<Float> foper = new SumValue<Float>();
    SumValue<Integer> ioper = new SumValue<Integer>();
    SumValue<Long> loper = new SumValue<Long>();
    SumValue<Short> soper = new SumValue<Short>();
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

  public void testNodeSchemaProcessing(SumValue oper, String debug)
  {
    TestSink sumSink = new TestSink();
    TestSink countSink = new TestSink();
    TestSink averageSink = new TestSink();
    oper.sum.setSink(sumSink);
    oper.count.setSink(countSink);
    oper.average.setSink(averageSink);

    // Not needed, but still setup is being called as a matter of discipline
    oper.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));
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

    Number dval = (Number) sumSink.collectedTuples.get(0);
    Integer count = (Integer) countSink.collectedTuples.get(0);
    Number average = (Number) averageSink.collectedTuples.get(0);
    log.debug(String.format("\nBenchmark %d tuples of type %s: total was %f; count was %d; average was %f",
                            numTuples * 3, debug, dval.doubleValue(), count, average.doubleValue()));
  }
}
