/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.MatchAllMap;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.MatchAllMap}<p>
 *
 */
public class MatchAllMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MatchAllMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MatchAllMap<String, Integer>());
    testNodeProcessingSchema(new MatchAllMap<String, Double>());
    testNodeProcessingSchema(new MatchAllMap<String, Float>());
    testNodeProcessingSchema(new MatchAllMap<String, Short>());
    testNodeProcessingSchema(new MatchAllMap<String, Long>());
  }

  public void testNodeProcessingSchema(MatchAllMap oper)
  {
    CountAndLastTupleTestSink matchSink = new CountAndLastTupleTestSink();
    oper.all.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, Number> input1 = new HashMap<String, Number>();
    HashMap<String, Number> input2 = new HashMap<String, Number>();
    HashMap<String, Number> input3 = new HashMap<String, Number>();
    input1.put("a", 3);
    input1.put("b", 20);
    input2.put("c", 1000);
    input2.put("a", 3);
    input3.put("a", 2);
    input3.put("b", 20);
    input3.put("c", 1000);

    int numTuples = 100000000;
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
    log.debug(String.format("\nBenchmark for %d tuples", numTuples*4));
  }
}
