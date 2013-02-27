/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.testbench.TestCountAndLastTupleSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.MatchAllStringMap}<p>
 *
 */
public class MatchAllStringMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MatchAllStringMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    MatchAllStringMap<String> oper = new MatchAllStringMap<String>();
    TestCountAndLastTupleSink matchSink = new TestCountAndLastTupleSink();
    oper.all.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, String> input1 = new HashMap<String, String>();
    HashMap<String, String> input2 = new HashMap<String, String>();
    HashMap<String, String> input3 = new HashMap<String, String>();
    input1.put("a", "3");
    input1.put("b", "20");
    input2.put("c", "1000");
    input2.put("a", "3");
    input3.put("a", "2");
    input3.put("b", "20");
    input3.put("c", "1000");

    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
    }
    oper.endWindow();

    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmark for %d tuples", numTuples*4));
  }
}
