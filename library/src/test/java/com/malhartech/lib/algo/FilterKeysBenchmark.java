/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.FilterKeys}<p>
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
  @Category(com.malhartech.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    FilterKeys<String, Number> oper = new FilterKeys<String, Number>();

    TestSink<HashMap<String, Number>> sortSink = new TestSink<HashMap<String, Number>>();
    oper.filter.setSink(sortSink);
    oper.setup(new OperatorConfiguration());
    ArrayList<String> keys = new ArrayList<String>();
    keys.add("e");
    keys.add("f");
    keys.add("blah");
    oper.setKey("a");
    oper.setKeys(keys);

    oper.beginWindow();
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
      sortSink.clear();
      input.clear();
      input.put("a", 5);
      oper.data.process(input);
      sortSink.clear();
      input.clear();
      input.put("a", 2);
      input.put("b", 33);
      input.put("f", 2);
      oper.data.process(input);
      sortSink.clear();

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
      sortSink.clear();

      input.clear();
      input.put("c", 9);
      oper.data.process(input);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmarked %d tuples", numTuples*5));
  }
}
