/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.Sigma;
import com.datatorrent.lib.testbench.SumTestSink;

import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.Sigma}<p>
 *
 */
public class SigmaTest
{
  private static Logger log = LoggerFactory.getLogger(SigmaTest.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeSchemaProcessing()
  {
    Sigma oper = new Sigma();
    SumTestSink lmultSink = new SumTestSink();
    SumTestSink imultSink = new SumTestSink();
    SumTestSink dmultSink = new SumTestSink();
    SumTestSink fmultSink = new SumTestSink();
    oper.longResult.setSink(lmultSink);
    oper.integerResult.setSink(imultSink);
    oper.doubleResult.setSink(dmultSink);
    oper.floatResult.setSink(fmultSink);

    int sum = 0;
    ArrayList<Integer> list = new ArrayList<Integer>();
    for (int i = 0; i < 100; i++) {
      list.add(i);
      sum += i;
    }

    oper.beginWindow(0); //
    oper.input.process(list);
    oper.endWindow(); //

    oper.beginWindow(1); //
    oper.input.process(list);
    oper.endWindow(); //
    sum = sum * 2;

    Assert.assertEquals("sum was", sum, lmultSink.val.intValue());
    Assert.assertEquals("sum was", sum, imultSink.val.intValue());
    Assert.assertEquals("sum was", sum, dmultSink.val.intValue());
    Assert.assertEquals("sum", sum, fmultSink.val.intValue());
  }
}
