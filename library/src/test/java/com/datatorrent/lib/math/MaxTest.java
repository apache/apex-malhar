/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.math.Max;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.Max}. <p>
 *
 */
public class MaxTest
{
  private static Logger LOG = LoggerFactory.getLogger(MaxTest.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeProcessing()
  {
    Max<Double> oper = new Max<Double>();
    TestSink rangeSink = new TestSink();
    oper.max.setSink(rangeSink);

    oper.beginWindow(0); //

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

    oper.endWindow(); //

    // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
    Assert.assertEquals("number emitted tuples", 1, rangeSink.collectedTuples.size());
    for (Object o: rangeSink.collectedTuples) { // sum is 1157
      Double val = (Double)o;
      Assert.assertEquals("emitted high value was ", new Double(1000.0), val);
    }
  }
}
