/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.testbench.CountAndLastTupleTestSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.MatchAnyMap}<p>
 *
 */
public class MatchAnyMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MatchAnyMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MatchAnyMap<String, Integer>());
    testNodeProcessingSchema(new MatchAnyMap<String, Double>());
    testNodeProcessingSchema(new MatchAnyMap<String, Float>());
    testNodeProcessingSchema(new MatchAnyMap<String, Short>());
    testNodeProcessingSchema(new MatchAnyMap<String, Long>());
  }

  public void testNodeProcessingSchema(MatchAnyMap oper)
  {
    CountAndLastTupleTestSink matchSink = new CountAndLastTupleTestSink();
    oper.any.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    HashMap<String, Number> input1 = new HashMap<String, Number>();
    HashMap<String, Number> input2 = new HashMap<String, Number>();
    HashMap<String, Number> input3 = new HashMap<String, Number>();

    input1.put("a", 2);
    input1.put("b", 20);
    input1.put("c", 1000);
    input2.put("a", 3);
    input3.put("a", 5);

    int numTuples = 10000000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
    }
    oper.endWindow();

    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow();
    // There should be no emit as all tuples do not match
    log.debug(String.format("\nBenchmarcked %d tuples", numTuples*4));
  }
}
