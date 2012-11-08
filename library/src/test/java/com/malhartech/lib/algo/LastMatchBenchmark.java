/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestCountAndLastTupleSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.LastMatch}<p>
 *
 */
public class LastMatchBenchmark
{
  private static Logger log = LoggerFactory.getLogger(LastMatchBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new LastMatch<String, Integer>());
    testNodeProcessingSchema(new LastMatch<String, Double>());
    testNodeProcessingSchema(new LastMatch<String, Float>());
    testNodeProcessingSchema(new LastMatch<String, Short>());
    testNodeProcessingSchema(new LastMatch<String, Long>());
  }

  public void testNodeProcessingSchema(LastMatch oper)
  {
    TestCountAndLastTupleSink matchSink = new TestCountAndLastTupleSink();
    oper.last.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3);
    oper.setTypeEQ();

    HashMap<String, Number> input = new HashMap<String, Number>();

    oper.beginWindow(0);
    matchSink.clear();

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      input.put("a", 4);
      input.put("b", 20);
      input.put("c", 1000);
      oper.data.process(input);
      input.put("a", 3);
      input.put("b", 20);
      input.put("c", 1000);
      oper.data.process(input);
      input.clear();
      input.put("a", 2);
      oper.data.process(input);
      input.clear();
      input.put("a", 4);
      input.put("b", 21);
      input.put("c", 1000);
      oper.data.process(input);
      input.clear();
      input.put("a", 3);
      input.put("b", 52);
      input.put("c", 5);
      oper.data.process(input);
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, matchSink.count);
    HashMap<String, Number> tuple = (HashMap<String, Number>)matchSink.tuple;
    Number aval = tuple.get("a");
    Number bval = tuple.get("b");
    Assert.assertEquals("Value of a was ", 3, aval.intValue());
    Assert.assertEquals("Value of b was ", 52, bval.intValue());

    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      matchSink.clear();
      input.clear();
      input.put("a", 2);
      input.put("b", 20);
      input.put("c", 1000);
      oper.data.process(input);
      input.clear();
      input.put("a", 5);
      oper.data.process(input);
    }
    oper.endWindow();
    // There should be no emit as all tuples do not match
    Assert.assertEquals("number emitted tuples", 0, matchSink.count);
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 7));
  }
}
