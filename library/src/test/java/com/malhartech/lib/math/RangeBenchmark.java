/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;
import com.malhartech.lib.util.HighLow;
import java.util.ArrayList;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.Range}<p>
 *
 */
public class RangeBenchmark
{
  private static Logger log = LoggerFactory.getLogger(RangeBenchmark.class);

  class TestSink implements Sink
  {
    Object tuple;

    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        tuple = payload;
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
    Range<Double> oper = new Range<Double>();
    TestSink rangeSink = new TestSink();
    oper.range.setSink(rangeSink);

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

    HighLow hl = (HighLow)rangeSink.tuple;
    log.debug(String.format("\nBenchmark total %d tuples was expected (1000,1) got (%f,%f)", numTuples * 12,
                            hl.getHigh().doubleValue(),
                            hl.getLow().doubleValue()));
  }
}
