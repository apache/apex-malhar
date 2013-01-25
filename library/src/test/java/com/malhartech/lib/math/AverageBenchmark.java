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
 * Performance tests for {@link com.malhartech.lib.math.AverageValue}. <p>
 * Current benchmark is 175 million tuples per second.
 *
 */
public class AverageBenchmark
{
  private static Logger log = LoggerFactory.getLogger(AverageBenchmark.class);

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
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    Average<Double> doper = new Average<Double>();
    Average<Float> foper = new Average<Float>();
    Average<Integer> ioper = new Average<Integer>();
    Average<Long> loper = new Average<Long>();
    Average<Short> soper = new Average<Short>();
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

  public void testNodeSchemaProcessing(Average oper, String debug)
  {
    TestSink averageSink = new TestSink();
    oper.average.setSink(averageSink);

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

    Number average = (Number) averageSink.collectedTuples.get(0);
    log.debug(String.format("\nBenchmark %d tuples of type %s: taverage was %f",
                            numTuples * 3, debug, average.doubleValue()));
  }
}
