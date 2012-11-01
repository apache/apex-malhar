/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.dag.TestCountAndLastTupleSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.MatchAll}<p>
 *
 */
public class MatchAllBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MatchAllBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MatchAll<String, Integer>());
    testNodeProcessingSchema(new MatchAll<String, Double>());
    testNodeProcessingSchema(new MatchAll<String, Float>());
    testNodeProcessingSchema(new MatchAll<String, Short>());
    testNodeProcessingSchema(new MatchAll<String, Long>());
  }

  public void testNodeProcessingSchema(MatchAll oper)
  {
    TestCountAndLastTupleSink matchSink = new TestCountAndLastTupleSink();
    oper.all.setSink(matchSink);
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null));
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow();
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

    oper.beginWindow();
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmark for %d tuples", numTuples*4));
  }
}