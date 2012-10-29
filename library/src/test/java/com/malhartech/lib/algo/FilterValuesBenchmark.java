/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestSink;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.FilterValues}<p>
 *
 */
public class FilterValuesBenchmark
{
  private static Logger log = LoggerFactory.getLogger(FilterValuesBenchmark.class);

  int getTotal(List list)
  {
    ArrayList<Integer> ilist = (ArrayList<Integer>)list;
    int ret = 0;
    for (Integer i: ilist) {
      ret += i.intValue();
    }
    return ret;
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    FilterValues<Integer> oper = new FilterValues<Integer>();

    TestSink<Integer> sortSink = new TestSink<Integer>();
    oper.filter.setSink(sortSink);
    oper.setup(new OperatorConfiguration());
    ArrayList<Integer> Values = new ArrayList<Integer>();
    oper.setValue(5);
    oper.clearValues();
    Values.add(200);
    Values.add(2);
    oper.setValue(4);
    oper.setValues(Values);

    oper.beginWindow();

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      oper.setInverse(false);
      sortSink.clear();
      oper.data.process(2);
      oper.data.process(5);
      oper.data.process(7);
      oper.data.process(42);
      oper.data.process(200);
      oper.data.process(2);
      oper.data.process(2);
      oper.data.process(33);
      oper.data.process(2);
      oper.data.process(6);
      oper.data.process(2);
      oper.data.process(6);
      oper.data.process(2);
      oper.data.process(6);
      oper.data.process(2);
      oper.data.process(6);
      oper.data.process(2);
      oper.setInverse(true);
      oper.data.process(9);
    }

    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 17));
    Assert.assertEquals("Sum of each round was ", 225, getTotal(sortSink.collectedTuples));
    oper.endWindow();
  }
}
