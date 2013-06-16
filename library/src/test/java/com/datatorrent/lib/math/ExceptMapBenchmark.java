/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.ExceptMap;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.ExceptMap}<p>
 *
 */
public class ExceptMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ExceptMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ExceptMap<String, Integer>());
    testNodeProcessingSchema(new ExceptMap<String, Double>());
    testNodeProcessingSchema(new ExceptMap<String, Float>());
    testNodeProcessingSchema(new ExceptMap<String, Short>());
    testNodeProcessingSchema(new ExceptMap<String, Long>());
  }

  public void testNodeProcessingSchema(ExceptMap oper)
  {
    CountAndLastTupleTestSink exceptSink = new CountAndLastTupleTestSink();
    oper.except.setSink(exceptSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    int numTuples = 10000000;
    HashMap<String, Number> input1 = new HashMap<String, Number>();
    HashMap<String, Number> input2 = new HashMap<String, Number>();
    input1.put("a", 2);
    input1.put("b", 20);
    input1.put("c", 1000);
    input2.put("a", 3);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
    }
    oper.endWindow();

    // One for each key
    log.debug(String.format("\nBenchmark, processed %d tuples", numTuples * 2));
  }
}