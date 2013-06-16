/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.algo.OrderByValue;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.OrderByValueDesc}<p>
 */
public class OrderByValueDescBenchmark
{
  private static Logger log = LoggerFactory.getLogger(OrderByValueDescBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    OrderByValue<String, Integer> oper = new OrderByValue<String, Integer>();
    TestSink countSink = new TestSink();
    TestSink listSink = new TestSink();
    oper.ordered_list.setSink(countSink);
    oper.ordered_count.setSink(listSink);
    String[] keys = new String[2];
    keys[0] = "a";
    keys[1] = "b";
    oper.setFilterBy(keys);
    oper.setInverse(false);
    oper.setup(null);

    HashMap<String, Integer> input = new HashMap<String, Integer>();
    int numTuples = 1000000;

    for (int i = 0; i < numTuples; i++) {

      oper.beginWindow(0);
      input.clear();
      input.put("a", 1);
      input.put("b", 5);
      input.put("c", 6);
      oper.data.process(input);

      input.clear();
      input.put("b", 50);
      input.put("c", 16);
      oper.data.process(input);

      input.clear();
      input.put("a", 1);
      input.put("b", 2);
      input.put("c", 3);
      oper.data.process(input);

      input.clear();
      input.put("a", i + 1);
      input.put("b", 7);
      input.put("c", 4);
      oper.data.process(input);

      input.clear();
      input.put("a", 3);
      input.put("b", 23);
      input.put("c", 33);
      oper.data.process(input);

      input.clear();
      input.put("a", -20);
      input.put("b", 5);
      input.put("c", 6);
      oper.data.process(input);

      input.clear();
      input.put("e", 2);
      input.put("b", 5);
      input.put("c", 6);
      oper.data.process(input);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmarked %d key,val pairs", numTuples * 20));
  }
}
