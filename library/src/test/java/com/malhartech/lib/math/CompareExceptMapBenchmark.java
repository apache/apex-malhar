/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.lib.testbench.TestCountAndLastTupleSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.CompareExceptMap}<p>
 *
 */
public class CompareExceptMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(CompareExceptMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new CompareExceptMap<String, Integer>());
    testNodeProcessingSchema(new CompareExceptMap<String, Double>());
    testNodeProcessingSchema(new CompareExceptMap<String, Float>());
    testNodeProcessingSchema(new CompareExceptMap<String, Short>());
    testNodeProcessingSchema(new CompareExceptMap<String, Long>());
  }

  public void testNodeProcessingSchema(CompareExceptMap oper)
  {
    TestCountAndLastTupleSink compareSink = new TestCountAndLastTupleSink();
    TestCountAndLastTupleSink exceptSink = new TestCountAndLastTupleSink();
    oper.compare.setSink(compareSink);
    oper.except.setSink(exceptSink);

    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, Number> input1 = new HashMap<String, Number>();
    HashMap<String, Number> input2 = new HashMap<String, Number>();

    input1.put("a", 2);
    input1.put("b", 20);
    input1.put("c", 1000);
    input2.put("a", 3);
    input2.put("b", 21);
    input2.put("c", 30);
    int numTuples = 10000000;

    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmark for %d tuples", numTuples*2));
  }
}