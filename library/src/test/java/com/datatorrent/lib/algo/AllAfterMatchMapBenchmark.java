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
 * Performance tests for {@link com.malhartech.lib.algo.AllAfterMatchMap} <p>
 *
 */
public class AllAfterMatchMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(AllAfterMatchMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new AllAfterMatchMap<String, Integer>());
    testNodeProcessingSchema(new AllAfterMatchMap<String, Double>());
    testNodeProcessingSchema(new AllAfterMatchMap<String, Float>());
    testNodeProcessingSchema(new AllAfterMatchMap<String, Short>());
    testNodeProcessingSchema(new AllAfterMatchMap<String, Long>());
  }

  public void testNodeProcessingSchema(AllAfterMatchMap oper)
  {
    CountAndLastTupleTestSink allSink = new CountAndLastTupleTestSink();
    oper.allafter.setSink(allSink);
    oper.setup(null);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, Number> input1 = new HashMap<String, Number>();
    HashMap<String, Number> input2 = new HashMap<String, Number>();
    HashMap<String, Number> input3 = new HashMap<String, Number>();
    HashMap<String, Number> input4 = new HashMap<String, Number>();

    input1.put("a", 2);
    input1.put("b", 20);
    input1.put("c", 1000);
    input2.put("a", 3);
    input3.put("b", 6);
    input4.put("c", 9);
    int numTuples = 10000000;

    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
      oper.data.process(input3);
      oper.data.process(input4);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 6));
  }
}
