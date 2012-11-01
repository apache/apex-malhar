/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.dag.TestCountAndLastTupleSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Perforamnce tests for {@link com.malhartech.lib.algo.AllAfterMatchStringValue} <p>
 *
 */
public class AllAfterMatchStringValueBenchmark
{
  private static Logger log = LoggerFactory.getLogger(AllAfterMatchStringValueBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    AllAfterMatchStringValue<String> oper = new AllAfterMatchStringValue<String>();
    TestCountAndLastTupleSink allSink = new TestCountAndLastTupleSink();
    oper.allafter.setSink(allSink);
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null));
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow();
    HashMap<String, String> input1 = new HashMap<String, String>();
    HashMap<String, String> input2 = new HashMap<String, String>();
    HashMap<String, String> input3 = new HashMap<String, String>();
    HashMap<String, String> input4 = new HashMap<String, String>();

    input1.put("a", "2");
    input1.put("b", "20");
    input1.put("c", "1000");
    input2.put("a", "3");
    input3.put("b", "6");
    input4.put("c", "9");

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
      oper.data.process(input3);
      oper.data.process(input4);
    }

    oper.endWindow();
    log.debug(String.format("\nBenchmarked %d tuples", numTuples*4));
  }
}
