/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.testbench.CountTestSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.MatchMap}<p>
 *
 */
public class MatchMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MatchMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MatchMap<String, Integer>());
    testNodeProcessingSchema(new MatchMap<String, Double>());
    testNodeProcessingSchema(new MatchMap<String, Float>());
    testNodeProcessingSchema(new MatchMap<String, Short>());
    testNodeProcessingSchema(new MatchMap<String, Long>());
  }

  public void testNodeProcessingSchema(MatchMap oper)
  {
    CountTestSink matchSink = new CountTestSink();
    oper.match.setSink(matchSink);
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
    log.debug(String.format("\nBenchmark, processed %d key/val pairs", numTuples * 4));
  }
}
