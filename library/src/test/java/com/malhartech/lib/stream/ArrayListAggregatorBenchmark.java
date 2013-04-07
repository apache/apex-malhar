/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.lib.testbench.CountTestSink;
import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.testbench.ArrayListAggregator}<p>
 */
public class ArrayListAggregatorBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ArrayListAggregatorBenchmark.class);

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    ArrayListAggregator<Integer> oper = new ArrayListAggregator<Integer>();
    CountTestSink cSink = new CountTestSink();

    oper.output.setSink(cSink);
    int size = 10;
    oper.setSize(size);
    oper.setFlushWindow(false);
    int numtuples = 100000000;

    oper.beginWindow(0);
    for (int i = 0; i < numtuples; i++) {
      oper.input.process(i);
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", numtuples/size, cSink.getCount());
    log.debug(String.format("\nProcessed %d tuples", numtuples));
  }
}
