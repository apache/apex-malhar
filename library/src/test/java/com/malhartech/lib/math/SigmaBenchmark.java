/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.lib.testbench.CountTestSink;
import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.Sigma}<p>
 *
 */
public class SigmaBenchmark
{
  private static Logger log = LoggerFactory.getLogger(SigmaBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing()
  {
    Sigma oper = new Sigma();
    CountTestSink lSink = new CountTestSink();
    CountTestSink iSink = new CountTestSink();
    CountTestSink dSink = new CountTestSink();
    CountTestSink fSink = new CountTestSink();
    oper.longResult.setSink(lSink);
    oper.integerResult.setSink(iSink);
    oper.doubleResult.setSink(dSink);
    oper.floatResult.setSink(fSink);

    int total = 100000000;
    ArrayList<Integer> list = new ArrayList<Integer>();
    for (int i = 0; i < 10; i++) {
      list.add(i);
    }

    for (int i = 0; i < total; i++) {
      oper.beginWindow(0); //
      oper.input.process(list);
      oper.endWindow(); //
    }
    Assert.assertEquals("sum was", total, lSink.getCount());
    Assert.assertEquals("sum was", total, iSink.getCount());
    Assert.assertEquals("sum was", total, dSink.getCount());
    Assert.assertEquals("sum was", total, fSink.getCount());

  }
}
