/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.COuntMap} <p>
 *
 */
public class CountMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(CountMapBenchmark.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing() throws InterruptedException
  {
    CountMap<String, Double> oper = new CountMap<String, Double>();
    TestSink countSink = new TestSink();
    oper.count.setSink(countSink);

    int numTuples = 100000000;
    oper.beginWindow(0);
    HashMap<String, Double> input = new HashMap<String, Double>();

    for (int i = 0; i < numTuples; i++) {
      input.put("a", 2.0);
      input.put("b", 20.0);
      input.put("c", 10.0);
      oper.data.process(input);
    }
    oper.endWindow();

    HashMap<String, Integer> dhash = (HashMap<String, Integer>) countSink.collectedTuples.get(0);
    log.debug(String.format("\nBenchmark sums for %d key/val pairs", numTuples * 3));

    log.debug(String.format("\nFor sum expected(%d,%d,%d), got(%d,%d,%d);",
                            2 * numTuples, 20 * numTuples, 10 * numTuples,
                            dhash.get("a").intValue(), dhash.get("b").intValue(), dhash.get("c").intValue()));
  }
}
