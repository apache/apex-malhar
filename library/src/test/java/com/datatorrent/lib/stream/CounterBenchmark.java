/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.Counter;
import com.datatorrent.lib.testbench.CountTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.datatorrent.lib.testbench.Counter}<p>
 */
public class CounterBenchmark
{
  private static Logger log = LoggerFactory.getLogger(CounterBenchmark.class);

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    Counter oper = new Counter();
    CountTestSink cSink = new CountTestSink();

    oper.output.setSink(cSink);
    int numtuples = 1000000000;

    oper.beginWindow(0);
    for (int i = 0; i < numtuples; i++) {
      oper.input.process(i);
    }
    oper.endWindow();

    oper.beginWindow(1);
    for (int i = 0; i < numtuples; i++) {
      oper.input.process(i);
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 2, cSink.getCount());
  }
}
