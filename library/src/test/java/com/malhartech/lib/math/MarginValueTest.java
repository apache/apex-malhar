/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestCountAndLastTupleSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.MarginValue}<p>
 *
 */
public class MarginValueTest
{
  private static Logger LOG = LoggerFactory.getLogger(MarginValueTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MarginValue<Integer>());
    testNodeProcessingSchema(new MarginValue<Double>());
    testNodeProcessingSchema(new MarginValue<Float>());
    testNodeProcessingSchema(new MarginValue<Short>());
    testNodeProcessingSchema(new MarginValue<Long>());
  }

  public void testNodeProcessingSchema(MarginValue oper)
  {
    TestCountAndLastTupleSink marginSink = new TestCountAndLastTupleSink();

    oper.margin.setSink(marginSink);
    oper.setPercent(true);
    oper.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));

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