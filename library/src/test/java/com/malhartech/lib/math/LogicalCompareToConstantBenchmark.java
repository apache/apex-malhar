  package com.malhartech.lib.math;

  /**
   * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
   */


import com.malhartech.lib.testbench.CountTestSink;
import com.malhartech.util.Pair;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

  /**
   *
   * Performance tests for {@link com.malhartech.lib.math.LogicalCompareToConstant}<p>
   *
   */
  public class LogicalCompareToConstantBenchmark
  {
    /**
     * Test operator logic emits correct results.
     */
    @Test
    @Category(com.malhartech.annotation.PerformanceTestCategory.class)
    public void testNodeProcessing()
    {
      LogicalCompareToConstant<Integer> oper = new LogicalCompareToConstant<Integer>() {};
      CountTestSink eSink = new CountTestSink();
      CountTestSink neSink = new CountTestSink();
      CountTestSink gtSink = new CountTestSink();
      CountTestSink gteSink = new CountTestSink();
      CountTestSink ltSink = new CountTestSink();
      CountTestSink lteSink = new CountTestSink();

      oper.equalTo.setSink(eSink);
      oper.notEqualTo.setSink(neSink);
      oper.greaterThan.setSink(gtSink);
      oper.greaterThanOrEqualTo.setSink(gteSink);
      oper.lessThan.setSink(ltSink);
      oper.lessThanOrEqualTo.setSink(lteSink);
      oper.setConstant(2);


      int itot = 100000000;
      oper.beginWindow(0); //

    for (int i = 0; i < itot; i++) {
    oper.input.process(1);
    oper.input.process(2);
    oper.input.process(3);
    }
      oper.endWindow(); //

      Assert.assertEquals("number emitted tuples", itot, eSink.getCount());
      Assert.assertEquals("number emitted tuples", 2*itot, neSink.getCount());
      Assert.assertEquals("number emitted tuples", itot, gtSink.getCount());
      Assert.assertEquals("number emitted tuples", 2*itot, gteSink.getCount());
      Assert.assertEquals("number emitted tuples", itot, ltSink.getCount());
      Assert.assertEquals("number emitted tuples", 2*itot, lteSink.getCount());
    }
  }
