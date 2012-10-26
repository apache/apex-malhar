/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestCountAndLastTupleSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MatchAllStringBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MatchAllStringBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    MatchAllString<String> oper = new MatchAllString<String>();
    TestCountAndLastTupleSink matchSink = new TestCountAndLastTupleSink();
    oper.all.setSink(matchSink);
    oper.setup(new OperatorConfiguration());
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow();
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

    oper.beginWindow();
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmark for %d tuples", numTuples*4));
  }
}