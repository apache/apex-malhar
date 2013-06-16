/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.testbench.CountTestSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.DistinctMap} <p>
 *
 */
public class DistinctMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(DistinctMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings({"SleepWhileInLoop", "rawtypes", "unchecked"})
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    DistinctMap<String, Number> oper = new DistinctMap<String, Number>();

    CountTestSink sortSink = new CountTestSink<HashMap<String, Number>>();
    oper.distinct.setSink((CountTestSink<Object>)sortSink);

    HashMap<String, Number> input = new HashMap<String, Number>();

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      oper.beginWindow(0);
      input.put("a", 2);
      oper.data.process(input);
      input.clear();
      input.put("a", 2);
      oper.data.process(input);

      input.clear();
      input.put("a", 1000);
      oper.data.process(input);

      input.clear();
      input.put("a", 5);
      oper.data.process(input);

      input.clear();
      input.put("a", 2);
      input.put("b", 33);
      oper.data.process(input);

      input.clear();
      input.put("a", 33);
      input.put("b", 34);
      oper.data.process(input);

      input.clear();
      input.put("b", 34);
      oper.data.process(input);

      input.clear();
      input.put("b", 6);
      input.put("a", 2);
      oper.data.process(input);
      input.clear();
      input.put("c", 9);
      oper.data.process(input);
      oper.endWindow();
    }
    log.debug(String.format("\nBenchmarked %d tuples with %d emits", numTuples*12, sortSink.getCount()));
  }
}
