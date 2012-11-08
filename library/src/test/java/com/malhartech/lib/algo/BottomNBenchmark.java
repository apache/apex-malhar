/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.BottonN} <p>
 *
 */
public class BottomNBenchmark
{
  private static Logger log = LoggerFactory.getLogger(BottomNBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new BottomN<String, Integer>());
    testNodeProcessingSchema(new BottomN<String, Double>());
    testNodeProcessingSchema(new BottomN<String, Float>());
    testNodeProcessingSchema(new BottomN<String, Short>());
    testNodeProcessingSchema(new BottomN<String, Long>());
  }

  public void testNodeProcessingSchema(BottomN oper)
  {
    TestSink<HashMap<String, Number>> sortSink = new TestSink<HashMap<String, Number>>();
    oper.bottom.setSink(sortSink);
    oper.setN(3);

    oper.beginWindow(0);
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
