/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestCountSink;
import com.malhartech.engine.TestSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.Change}<p>
 *
 */
public class ChangeBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ChangeBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Change<String, Integer>());
    testNodeProcessingSchema(new Change<String, Double>());
    testNodeProcessingSchema(new Change<String, Float>());
    testNodeProcessingSchema(new Change<String, Short>());
    testNodeProcessingSchema(new Change<String, Long>());
  }

  public void testNodeProcessingSchema(Change oper)
  {
    TestCountSink changeSink = new TestCountSink();
    TestCountSink percentSink = new TestCountSink();

    oper.change.setSink(changeSink);
    oper.percent.setSink(percentSink);

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 2);
    input.put("b", 10);
    input.put("c", 100);
    oper.base.process(input);

    int numTuples = 1000000;

    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.put("a", 3);
      input.put("b", 2);
      input.put("c", 4);
      oper.data.process(input);

      input.clear();
      input.put("a", 4);
      input.put("b", 19);
      input.put("c", 150);
      oper.data.process(input);
    }

    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", numTuples * 6, changeSink.getCount());
    Assert.assertEquals("number emitted tuples", numTuples * 6, percentSink.getCount());
    log.debug(String.format("\nBenchmarked %d key,val pairs", numTuples * 6));
  }
}