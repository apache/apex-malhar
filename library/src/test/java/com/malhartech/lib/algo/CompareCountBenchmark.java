/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestCountAndLastTupleSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.CompareCount} <p>
 *
 */
public class CompareCountBenchmark
{
  private static Logger log = LoggerFactory.getLogger(CompareCountBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new CompareCount<String, Integer>());
    testNodeProcessingSchema(new CompareCount<String, Double>());
    testNodeProcessingSchema(new CompareCount<String, Float>());
    testNodeProcessingSchema(new CompareCount<String, Short>());
    testNodeProcessingSchema(new CompareCount<String, Long>());
  }

  public void testNodeProcessingSchema(CompareCount oper)
  {
    TestCountAndLastTupleSink countSink = new TestCountAndLastTupleSink();
    TestCountAndLastTupleSink exceptSink = new TestCountAndLastTupleSink();

    oper.count.setSink(countSink);
    oper.except.setSink(exceptSink);

    oper.setup(new OperatorConfiguration());
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();
    oper.beginWindow();

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
    log.debug(String.format("\nBenchmarked %d tuples", numTuples*2));
  }
}