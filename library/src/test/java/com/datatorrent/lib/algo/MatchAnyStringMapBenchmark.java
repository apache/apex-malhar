/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.MatchAnyStringMap;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.MatchAnyStringMap}<p>
 *
 */
public class MatchAnyStringMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MatchAnyStringMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    MatchAnyStringMap<String> oper = new MatchAnyStringMap<String>();
    CountAndLastTupleTestSink matchSink = new CountAndLastTupleTestSink();
    oper.any.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    HashMap<String, String> input1 = new HashMap<String, String>();
    HashMap<String, String> input2 = new HashMap<String, String>();
    HashMap<String, String> input3 = new HashMap<String, String>();

    input1.put("a", "3");
    input1.put("b", "20");
    input1.put("c", "1000");
    input2.put("a", "2");
    input3.put("a", "5");

    int numTuples = 100000000;
    oper.beginWindow(0);
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
    // There should be no emit as all tuples do not match
    log.debug(String.format("\nBenchmarcked %d tuples", numTuples*4));
  }
}
