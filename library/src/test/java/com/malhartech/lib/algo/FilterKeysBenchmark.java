/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestCountSink;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.FilterKeysMap}<p>
 *
 */
public class FilterKeysBenchmark
{
  private static Logger log = LoggerFactory.getLogger(FilterKeysBenchmark.class);

  int getTotal(Object o)
  {
    HashMap<String, Number> map = (HashMap<String, Number>)o;
    int ret = 0;
    for (Map.Entry<String, Number> e: map.entrySet()) {
      ret += e.getValue().intValue();
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
    FilterKeysMap<String, Number> oper = new FilterKeysMap<String, Number>();

    TestCountSink<HashMap<String, Number>> sortSink = new TestCountSink<HashMap<String, Number>>();
    oper.filter.setSink(sortSink);
    String [] keys = new String[3];
    keys[0] = "e";
    keys[1] = "f";
    keys[2] = "blah";
    oper.setKey("a");
    oper.setKeys(keys);

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();

    int numTuples = 10000000;

    for (int i = 0; i < numTuples; i++) {
      input.put("a", 2);
      input.put("b", 5);
      input.put("c", 7);
      input.put("d", 42);
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
      input.put("dd", 9);
      input.put("a", 9);
      oper.data.process(input);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmarked %d tuples with %d emitted", numTuples * 20, sortSink.getCount()));
  }
}
