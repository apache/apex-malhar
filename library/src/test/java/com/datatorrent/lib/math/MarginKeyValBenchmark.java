/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.MarginKeyVal;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import com.datatorrent.lib.util.KeyValPair;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.MarginKeyVal}. <p>
 *
 */
public class MarginKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MarginKeyValBenchmark.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    MarginKeyVal<String, Integer> oper = new MarginKeyVal<String, Integer>();

    CountAndLastTupleTestSink marginSink = new CountAndLastTupleTestSink();

    oper.margin.setSink(marginSink);
    oper.beginWindow(0);

    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
      oper.numerator.process(new KeyValPair("a", new Integer(2)));
      oper.numerator.process(new KeyValPair("b", new Integer(20)));
      oper.numerator.process(new KeyValPair("c", new Integer(1000)));
      oper.denominator.process(new KeyValPair("a", new Integer(2)));
      oper.denominator.process(new KeyValPair("b", new Integer(40)));
      oper.denominator.process(new KeyValPair("c", new Integer(500)));
    }

    oper.endWindow();
    log.debug(String.format("number emitted tuples are %d", numTuples * 6));
    KeyValPair<String, Number> output = (KeyValPair<String, Number>)marginSink.tuple;
    log.debug(String.format("Key, value is %s,%f", output.getKey(), output.getValue().doubleValue()));
  }
}