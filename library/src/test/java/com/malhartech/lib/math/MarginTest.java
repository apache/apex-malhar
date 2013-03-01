/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.lib.testbench.CountAndLastTupleTestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.Margin}<p>
 *
 */
public class MarginTest
{
  private static Logger LOG = LoggerFactory.getLogger(MarginTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Margin<Integer>());
    testNodeProcessingSchema(new Margin<Double>());
    testNodeProcessingSchema(new Margin<Float>());
    testNodeProcessingSchema(new Margin<Short>());
    testNodeProcessingSchema(new Margin<Long>());
  }

  public void testNodeProcessingSchema(Margin oper)
  {
    CountAndLastTupleTestSink marginSink = new CountAndLastTupleTestSink();

    oper.margin.setSink(marginSink);
    oper.setPercent(true);

    oper.beginWindow(0);
    oper.numerator.process(2);
    oper.numerator.process(20);
    oper.numerator.process(100);
    oper.denominator.process(200);
    oper.denominator.process(22);
    oper.denominator.process(22);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, marginSink.count);
    Assert.assertEquals("margin was ", 50, ((Number) marginSink.tuple).intValue());

    marginSink.clear();
    oper.beginWindow(0);
    oper.numerator.process(2);
    oper.numerator.process(20);
    oper.numerator.process(100);
    oper.denominator.process(17);
    oper.denominator.process(22);
    oper.denominator.process(22);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, marginSink.count);
    Assert.assertEquals("margin was ", -100, ((Number) marginSink.tuple).intValue());

    marginSink.clear();
    oper.beginWindow(0);
    oper.numerator.process(2);
    oper.numerator.process(20);
    oper.numerator.process(100);
    oper.denominator.process(17);
    oper.denominator.process(22);
    oper.denominator.process(-39);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 0, marginSink.count);
  }
}