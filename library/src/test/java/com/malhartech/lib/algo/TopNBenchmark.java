/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.dag.TestSink;
import java.util.HashMap;
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
public class TopNBenchmark
{
  private static Logger log = LoggerFactory.getLogger(TopNBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new TopN<String, Integer>());
    testNodeProcessingSchema(new TopN<String, Double>());
    testNodeProcessingSchema(new TopN<String, Float>());
    testNodeProcessingSchema(new TopN<String, Short>());
    testNodeProcessingSchema(new TopN<String, Long>());
  }

  public void testNodeProcessingSchema(TopN oper)
  {
    TestSink<HashMap<String, Number>> sortSink = new TestSink<HashMap<String, Number>>();
    oper.top.setSink(sortSink);
    oper.setN(3);

    oper.beginWindow();
    HashMap<String, Number> input = new HashMap<String, Number>();

    int numTuples = 5000000;
    for (int i = 0; i < numTuples; i++) {
      input.put("a", i);
      input.put("b", numTuples - i);
      oper.data.process(input);
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 2, sortSink.collectedTuples.size());
    log.debug(String.format("\nBenchmaked %d tuples", numTuples));
  }
}
