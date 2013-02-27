/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.testbench.TestCountSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.TopN}<p>
 *
 */
public class TopNUniqueBenchmark
{
  private static Logger log = LoggerFactory.getLogger(TopNUniqueBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new TopNUnique<String, Integer>());
    testNodeProcessingSchema(new TopNUnique<String, Double>());
    testNodeProcessingSchema(new TopNUnique<String, Float>());
    testNodeProcessingSchema(new TopNUnique<String, Short>());
    testNodeProcessingSchema(new TopNUnique<String, Long>());
  }

  public void testNodeProcessingSchema(TopNUnique oper)
  {
    TestCountSink<HashMap<String, Number>> sortSink = new TestCountSink<HashMap<String, Number>>();
    oper.top.setSink(sortSink);
    oper.setN(3);

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();

    int numTuples = 5000000;
    for (int j = 0; j < numTuples / 1000; j++) {
      for (int i = 999; i >= 0; i--) {
        input.put("a", i);
        input.put("b", numTuples - i);
        oper.data.process(input);
      }
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 2, sortSink.getCount());
    log.debug(String.format("\nBenchmaked %d tuples", numTuples * 2));
  }
}
