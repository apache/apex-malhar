/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestCountAndLastTupleSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.CompareExceptCount} <p>
 *
 */
public class CompareExceptCountBenchmark
{
  private static Logger log = LoggerFactory.getLogger(CompareExceptCountBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new CompareExceptCount<String, Integer>());
    testNodeProcessingSchema(new CompareExceptCount<String, Double>());
    testNodeProcessingSchema(new CompareExceptCount<String, Float>());
    testNodeProcessingSchema(new CompareExceptCount<String, Short>());
    testNodeProcessingSchema(new CompareExceptCount<String, Long>());
  }

  public void testNodeProcessingSchema(CompareExceptCount oper)
  {
    TestCountAndLastTupleSink countSink = new TestCountAndLastTupleSink();
    TestCountAndLastTupleSink exceptSink = new TestCountAndLastTupleSink();

    oper.count.setSink(countSink);
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

    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmarked %d tuples", numTuples*4));
  }
}
