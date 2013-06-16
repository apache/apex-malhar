/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.SquareCalculus;
import com.datatorrent.lib.testbench.SumTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.SquareCalculus}<p>
 *
 */
public class SquareCalculusTest
{
  private static Logger log = LoggerFactory.getLogger(SquareCalculusTest.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeSchemaProcessing()
  {
    SquareCalculus oper = new SquareCalculus();
    SumTestSink lmultSink = new SumTestSink();
    SumTestSink imultSink = new SumTestSink();
    SumTestSink dmultSink = new SumTestSink();
    SumTestSink fmultSink = new SumTestSink();
    oper.longResult.setSink(lmultSink);
    oper.integerResult.setSink(imultSink);
    oper.doubleResult.setSink(dmultSink);
    oper.floatResult.setSink(fmultSink);

    oper.beginWindow(0); //
    int sum = 0;
    for (int i = 0; i < 50; i++) {
      Integer t = i;
      oper.input.process(t);
      sum += i * i;
    }
    oper.endWindow(); //

    Assert.assertEquals("sum was", sum, lmultSink.val.intValue());
    Assert.assertEquals("sum was", sum, imultSink.val.intValue());
    Assert.assertEquals("sum was", sum, dmultSink.val.intValue());
    Assert.assertEquals("sum", sum, fmultSink.val.intValue());
  }
}
