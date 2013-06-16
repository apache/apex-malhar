/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.MarginMap;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.MarginMap}<p>
 *
 */
public class MarginMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MarginMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MarginMap<String, Integer>());
  }

  public void testNodeProcessingSchema(MarginMap oper)
  {
    CountAndLastTupleTestSink marginSink = new CountAndLastTupleTestSink();

    oper.margin.setSink(marginSink);
    oper.beginWindow(0);
    HashMap<String, Number> ninput = new HashMap<String, Number>();
    HashMap<String, Number> dinput = new HashMap<String, Number>();

    int numTuples = 100000000;
    ninput.put("a", 2);
    ninput.put("b", 20);
    ninput.put("c", 1000);
    dinput.put("a", 2);
    dinput.put("b", 40);
    dinput.put("c", 500);

    for (int i = 0; i < numTuples; i++) {
      oper.numerator.process(ninput);
      oper.denominator.process(dinput);
    }

    oper.endWindow();
    log.debug(String.format("number emitted tuples are %d", numTuples * 6));
    HashMap<String, Number> output = (HashMap<String, Number>)marginSink.tuple;
    for (Map.Entry<String, Number> e: output.entrySet()) {
      log.debug(String.format("Key, value is %s,%f", e.getKey(), e.getValue().doubleValue()));
    }
  }
}