/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestSink;
import com.malhartech.lib.testbench.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.testbench.EventGenerator}. <p>
 * <br>
 * Load is generated and the tuples are outputted to ensure that the numbers are roughly in line with the weights<br>
 * <br>
 * Benchmarks:<br>
 * String schema generates over 11 Million tuples/sec<br>
 * HashMap schema generates over 1.7 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 *
 */
public class TopNUniqueBenchmark
{
  private static Logger log = LoggerFactory.getLogger(TopNUniqueBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new TopNUnique<String, Integer>());
    testNodeProcessingSchema(new TopNUnique<String, Double>());
    testNodeProcessingSchema(new TopNUnique<String, Float>());
    testNodeProcessingSchema(new TopNUnique<String, Short>());
    testNodeProcessingSchema(new TopNUnique<String, Long>());
  }

  public void testNodeProcessingSchema(TopNUnique oper)
  {
    TestSink<HashMap<String, Number>> sortSink = new TestSink<HashMap<String, Number>>();
    oper.top.setSink(sortSink);
    oper.setup(new OperatorConfiguration());
    oper.setN(3);

    oper.beginWindow();
    HashMap<String, Number> input = new HashMap<String, Number>();

    int numTuples = 5000000;
    for (int j = 0; j < numTuples / 1000; j++) {
      for (int i = 999; i >= 0; i--) {
        input.put("a", i);
        input.put("b", numTuples - i);
        oper.data.process(input);
      }
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 2, sortSink.collectedTuples.size());
    log.debug(String.format("\nBenchmaked %d tuples", numTuples));
    for (Object o: sortSink.collectedTuples) {
      for (Map.Entry<String, ArrayList<HashMap<Number, Integer>>> e: ((HashMap<String, ArrayList<HashMap<Number, Integer>>>)o).entrySet()) {
        log.debug(String.format("Sorted list for %s:", e.getKey()));
        for (HashMap<Number, Integer> ival: e.getValue()) {
          for (Map.Entry<Number, Integer> ie: ival.entrySet()) {
            log.debug(String.format("%s occurs %d times", ie.getKey().toString(), ie.getValue()));
          }
        }
      }
    }
  }
}
