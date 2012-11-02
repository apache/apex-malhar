/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.dag.TestCountAndLastTupleSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.Compare} <p>
 *
 */
public class CompareBenchmark
{
  private static Logger log = LoggerFactory.getLogger(CompareBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Compare<String, Integer>());
    testNodeProcessingSchema(new Compare<String, Double>());
    testNodeProcessingSchema(new Compare<String, Float>());
    testNodeProcessingSchema(new Compare<String, Short>());
    testNodeProcessingSchema(new Compare<String, Long>());
  }

  public void testNodeProcessingSchema(Compare oper)
  {
    TestCountAndLastTupleSink matchSink = new TestCountAndLastTupleSink();
    oper.compare.setSink(matchSink);
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null));
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeNEQ();

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