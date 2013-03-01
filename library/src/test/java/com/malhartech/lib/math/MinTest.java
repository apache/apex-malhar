/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.lib.testbench.CountAndLastTupleTestSink;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.Min}<p>
 *
 */
public class MinTest
{
  private static Logger log = LoggerFactory.getLogger(SumCountMap.class);


  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeSchemaProcessing()
  {
    Min<Double> oper = new Min<Double>();
    CountAndLastTupleTestSink minSink = new CountAndLastTupleTestSink();
    oper.min.setSink(minSink);

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
    Assert.assertEquals("number emitted tuples", 1, minSink.count);
    Assert.assertEquals("emitted high value was ", new Double(1.0), minSink.tuple);
  }
}
