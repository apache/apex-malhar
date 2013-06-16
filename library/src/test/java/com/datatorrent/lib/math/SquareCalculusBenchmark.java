/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.SquareCalculus;
import com.datatorrent.lib.testbench.CountTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.SquareCalculus}<p>
 *
 */
public class SquareCalculusBenchmark
{
  private static Logger log = LoggerFactory.getLogger(SquareCalculusBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing()
  {
    SquareCalculus oper = new SquareCalculus();
    CountTestSink lmultSink = new CountTestSink();
    CountTestSink imultSink = new CountTestSink();
    CountTestSink dmultSink = new CountTestSink();
    CountTestSink fmultSink = new CountTestSink();
    oper.longResult.setSink(lmultSink);
    oper.integerResult.setSink(imultSink);
    oper.doubleResult.setSink(dmultSink);
    oper.floatResult.setSink(fmultSink);


    oper.beginWindow(0); //
    int jtot = 100000;
    int itot = 1000;

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
