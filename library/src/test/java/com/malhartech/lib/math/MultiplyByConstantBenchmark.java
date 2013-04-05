/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.lib.testbench.CountTestSink;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.MultiplyByConstact}<p>
 *
 */
public class MultiplyByConstantBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MultiplyByConstantBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing()
  {
    MultiplyByConstant oper = new MultiplyByConstant();
    CountTestSink lmultSink = new CountTestSink();
    CountTestSink imultSink = new CountTestSink();
    CountTestSink dmultSink = new CountTestSink();
    CountTestSink fmultSink = new CountTestSink();
    oper.longProduct.setSink(lmultSink);
    oper.integerProduct.setSink(imultSink);
    oper.doubleProduct.setSink(dmultSink);
    oper.floatProduct.setSink(fmultSink);
    int imby = 2;
    Integer mby = imby;
    oper.setMultiplier(mby);

    oper.beginWindow(0); //
    int jtot = 1000;
    int itot = 100000;

    for (int j = 0; j < jtot; j++) {
      for (int i = 0; i < itot; i++) {
        Integer t = i;
        oper.input.process(t);
      }
    }
    oper.endWindow();
    Assert.assertEquals("number of tuples", jtot * itot, lmultSink.count);
    Assert.assertEquals("number of tuples", jtot * itot, imultSink.count);
    Assert.assertEquals("number of tuples", jtot * itot, dmultSink.count);
    Assert.assertEquals("number of tuples", jtot * itot, fmultSink.count);
  }
}
