/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.lib.testbench.TestCountSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.ChangeMap}. <p>
 *
 */
public class ChangeMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ChangeMapBenchmark.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeMap<String, Integer>());
    testNodeProcessingSchema(new ChangeMap<String, Double>());
    testNodeProcessingSchema(new ChangeMap<String, Float>());
    testNodeProcessingSchema(new ChangeMap<String, Short>());
    testNodeProcessingSchema(new ChangeMap<String, Long>());
  }

  public <V extends Number> void testNodeProcessingSchema(ChangeMap<String, V> oper)
  {
    TestCountSink<HashMap<String, V>> changeSink = new TestCountSink<HashMap<String, V>>();
    TestCountSink<HashMap<String, Double>> percentSink = new TestCountSink<HashMap<String, Double>>();

    oper.change.setSink(changeSink);
    oper.percent.setSink(percentSink);

    oper.beginWindow(0);
    HashMap<String, V> input = new HashMap<String, V>();
    input.put("a", oper.getValue(2));
    input.put("b", oper.getValue(10));
    input.put("c", oper.getValue(100));
    oper.base.process(input);

    int numTuples = 1000000;

    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.put("a", oper.getValue(3));
      input.put("b", oper.getValue(2));
      input.put("c", oper.getValue(4));
      oper.data.process(input);

      input.clear();
      input.put("a", oper.getValue(4));
      input.put("b", oper.getValue(19));
      input.put("c", oper.getValue(150));
      oper.data.process(input);
    }

    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", numTuples * 6, changeSink.getCount());
    Assert.assertEquals("number emitted tuples", numTuples * 6, percentSink.getCount());
    log.debug(String.format("\nBenchmarked %d key,val pairs", numTuples * 6));
  }
}