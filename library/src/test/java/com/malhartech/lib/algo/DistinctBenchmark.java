/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.Distinct} <p>
 *
 */
public class DistinctBenchmark
{
  private static Logger log = LoggerFactory.getLogger(DistinctBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    Distinct<String, Number> oper = new Distinct<String, Number>();

    TestSink<HashMap<String, Number>> sortSink = new TestSink<HashMap<String, Number>>();
    oper.distinct.setSink(sortSink);
    oper.setup(new OperatorConfiguration());

    HashMap<String, Number> input = new HashMap<String, Number>();

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      oper.beginWindow();
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
      sortSink.clear();
    }
    log.debug(String.format("\nBenchmarked %d tuples", 8*numTuples));
  }
}
