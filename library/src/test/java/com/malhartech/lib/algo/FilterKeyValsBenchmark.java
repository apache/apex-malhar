/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestCountSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.FilterKeyVals}<p>
 *
 */
public class FilterKeyValsBenchmark
{
  private static Logger log = LoggerFactory.getLogger(FilterKeyValsBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    FilterKeyVals<String, Number> oper = new FilterKeyVals<String, Number>();

    TestCountSink<HashMap<String, Number>> sortSink = new TestCountSink<HashMap<String, Number>>();
    oper.filter.setSink(sortSink);
    HashMap<String, Number> filter = new HashMap<String, Number>();
    filter.put("b", 2);
    oper.setKeyVals(filter);
    oper.clearKeys();

    filter.clear();
    filter.put("e", 200);
    filter.put("f", 2);
    filter.put("blah", 2);
    oper.setKeyVals(filter);
    filter.clear();
    filter.put("a", 2);
    oper.setKeyVals(filter);

    oper.beginWindow(0);
    int numTuples = 10000000;
    HashMap<String, Number> input = new HashMap<String, Number>();
    for (int i = 0; i < numTuples; i++) {
      oper.setInverse(false);
      input.put("a", 2);
      input.put("b", 5);
      input.put("c", 7);
      input.put("d", 42);
      input.put("e", 202);
      input.put("e", 200);
      input.put("f", 2);
      oper.data.process(input);

      input.clear();
      input.put("a", 5);
      oper.data.process(input);
      input.clear();
      input.put("a", 2);
      input.put("b", 33);
      input.put("f", 2);
      oper.data.process(input);

      input.clear();
      input.put("b", 6);
      input.put("a", 2);
      input.put("j", 6);
      input.put("e", 2);
      input.put("dd", 6);
      input.put("blah", 2);
      input.put("another", 6);
      input.put("notmakingit", 2);
      oper.data.process(input);

      input.clear();
      input.put("c", 9);
      oper.setInverse(true);
      oper.data.process(input);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmarked %d tuples with %d emitted", numTuples * 20, sortSink.getCount()));
  }
}
