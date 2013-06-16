/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.Sink;
import com.malhartech.lib.testbench.CountTestSink;
import com.malhartech.lib.testbench.HashTestSink;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.Distinct<p>
 *
 */
public class DistinctBenchmark
{
  private static Logger log = LoggerFactory.getLogger(DistinctBenchmark.class);
  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings({"SleepWhileInLoop", "rawtypes", "unchecked"})
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    Distinct<Integer> oper = new Distinct<Integer>();

    CountTestSink<Integer> sortSink = new CountTestSink<Integer>();
    Sink s = sortSink;
    oper.distinct.setSink((Sink<Object>)s);

    oper.beginWindow(0);
    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(1);
      oper.data.process(i % 13);
      oper.data.process(1);
      oper.data.process(i % 5);
      oper.data.process(1);

      oper.data.process(i % 20);
      oper.data.process(i % 2);
      oper.data.process(2);
      oper.data.process(i % 3);
      oper.data.process(1);

      oper.data.process(i % 5);
      oper.data.process(i % 10);
      oper.data.process(i % 25);
      oper.data.process(1);
      oper.data.process(3);

      oper.data.process(i % 4);
      oper.data.process(3);
      oper.data.process(1);
      oper.data.process(3);
      oper.data.process(4);
      oper.data.process(i);
    }
    oper.endWindow();

    //Assert.assertEquals("number emitted tuples", 4, sortSink.size());
    log.debug(String.format("\nBenchmarked %d tuples (emitted %d tupled)",
                            numTuples*20,
                            sortSink.getCount()));
  }
}
