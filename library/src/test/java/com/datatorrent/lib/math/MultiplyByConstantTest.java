/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.MultiplyByConstant;
import com.datatorrent.lib.testbench.SumTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.MultiplyByConstact}<p>
 *
 */
public class MultiplyByConstantTest
{
  private static Logger log = LoggerFactory.getLogger(MultiplyByConstantTest.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeSchemaProcessing()
  {
    MultiplyByConstant oper = new MultiplyByConstant();
    SumTestSink lmultSink = new SumTestSink();
    SumTestSink imultSink = new SumTestSink();
    SumTestSink dmultSink = new SumTestSink();
    SumTestSink fmultSink = new SumTestSink();
    oper.longProduct.setSink(lmultSink);
    oper.integerProduct.setSink(imultSink);
    oper.doubleProduct.setSink(dmultSink);
    oper.floatProduct.setSink(fmultSink);
    int imby = 2;
    Integer mby = imby;
    oper.setMultiplier(mby);

    oper.beginWindow(0); //

    int sum = 0;
    for (int i = 0; i < 100; i++) {
      Integer t = i;
      oper.input.process(t);
      sum += i * imby;
    }

    oper.endWindow(); //

    Assert.assertEquals("sum was", sum, lmultSink.val.intValue());
    Assert.assertEquals("sum was", sum, imultSink.val.intValue());
    Assert.assertEquals("sum was", sum, dmultSink.val.intValue());
    Assert.assertEquals("sum", sum, fmultSink.val.intValue());
  }
}
