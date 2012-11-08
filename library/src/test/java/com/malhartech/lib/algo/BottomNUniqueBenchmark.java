/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.BottomNUnique} <p>
 */
public class BottomNUniqueBenchmark
{
  private static Logger log = LoggerFactory.getLogger(BottomNUniqueBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new BottomNUnique<String, Integer>());
    testNodeProcessingSchema(new BottomNUnique<String, Double>());
    testNodeProcessingSchema(new BottomNUnique<String, Float>());
    testNodeProcessingSchema(new BottomNUnique<String, Short>());
    testNodeProcessingSchema(new BottomNUnique<String, Long>());
  }

  public void testNodeProcessingSchema(BottomNUnique oper)
  {
    TestSink<HashMap<String, Number>> sortSink = new TestSink<HashMap<String, Number>>();
    oper.bottom.setSink(sortSink);
    oper.setN(3);

    oper.beginWindow(0);
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
