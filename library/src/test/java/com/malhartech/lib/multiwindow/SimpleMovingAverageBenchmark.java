/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.multiwindow;

import com.malhartech.lib.testbench.CountTestSink;
import com.malhartech.common.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.multiwindow.SimpleMovingAverage}. <p>
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class SimpleMovingAverageBenchmark
{
  private static final Logger logger = LoggerFactory.getLogger(SimpleMovingAverageBenchmark.class);

  /**
   * Test functional logic.
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws InterruptedException
  {
    SimpleMovingAverage<String, Double> oper = new SimpleMovingAverage<String, Double>();

    CountTestSink sink = new CountTestSink<KeyValPair<String, Double>>();
    oper.doubleSMA.setSink(sink);
    oper.setWindowSize(3);

    long numTuples = 1000000;
    for (int i = 0; i < numTuples; ++i) {
      double val = 30;
      double val2 = 51;
      oper.beginWindow(0);
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.endWindow();

      oper.beginWindow(1);
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.endWindow();

      oper.beginWindow(2);
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.endWindow();

      oper.beginWindow(3);
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.endWindow();
    }

    Assert.assertEquals("number emitted tuples", numTuples * 8, sink.count);
    logger.debug(String.format("\nBenchmarked tuple count %d", numTuples * 8));

  }
}
