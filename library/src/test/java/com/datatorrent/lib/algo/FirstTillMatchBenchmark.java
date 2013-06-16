/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.FirstTillMatch;
import com.malhartech.engine.TestSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.FirstTillMatch}<p>
 *
 */
public class FirstTillMatchBenchmark
{
  private static Logger log = LoggerFactory.getLogger(FirstTillMatchBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new FirstTillMatch<String, Integer>());
    testNodeProcessingSchema(new FirstTillMatch<String, Double>());
    testNodeProcessingSchema(new FirstTillMatch<String, Float>());
    testNodeProcessingSchema(new FirstTillMatch<String, Short>());
    testNodeProcessingSchema(new FirstTillMatch<String, Long>());
  }

  @SuppressWarnings("unchecked")
  public void testNodeProcessingSchema(FirstTillMatch oper)
  {
    TestSink matchSink = new TestSink();
    oper.first.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3);
    oper.setTypeEQ();

    int numTuples = 1000000;
    for (int i = 0; i < numTuples; i++) {
      matchSink.clear();
      oper.beginWindow(0);
      HashMap<String, Number> input = new HashMap<String, Number>();
      input.put("a", 4);
      input.put("b", 20);
      input.put("c", 1000);
      oper.data.process(input);
      input.clear();
      input.put("a", 2);
      oper.data.process(input);
      input.put("a", 3);
      input.put("b", 20);
      input.put("c", 1000);
      oper.data.process(input);
      input.clear();
      input.put("a", 4);
      input.put("b", 21);
      input.put("c", 1000);
      oper.data.process(input);
      input.clear();
      input.put("a", 6);
      input.put("b", 20);
      input.put("c", 5);
      oper.data.process(input);
      oper.endWindow();
    }

    Assert.assertEquals("number emitted tuples", 2, matchSink.collectedTuples.size());
    int atotal = 0;
    Number aval;
    for (Object o: matchSink.collectedTuples) {
      aval = ((HashMap<String, Number>)o).get("a");
      atotal += aval.intValue();
    }
    Assert.assertEquals("Value of a was ", 6, atotal);
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 13));
  }

}
