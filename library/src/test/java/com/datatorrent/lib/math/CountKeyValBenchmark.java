/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.math.CountKeyVal;
import com.datatorrent.lib.util.KeyValPair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.CountKeyVal}<p>
 *
 */
public class CountKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(CountKeyValBenchmark.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing() throws InterruptedException
  {
    CountKeyVal<String, Double> oper = new CountKeyVal<String, Double>();
    TestSink countSink = new TestSink();
    oper.count.setSink(countSink);

    int numTuples = 100000000;
    oper.beginWindow(0);

    for (int i = 0; i < numTuples; i++) {
      oper.data.process(new KeyValPair("a", 2.0));
      oper.data.process(new KeyValPair("b", 20.0));
      oper.data.process(new KeyValPair("c", 10.0));
    }
    oper.endWindow();

    KeyValPair<String, Integer> c1 = (KeyValPair<String, Integer>)countSink.collectedTuples.get(0);
    KeyValPair<String, Integer> c2 = (KeyValPair<String, Integer>)countSink.collectedTuples.get(1);
    KeyValPair<String, Integer> c3 = (KeyValPair<String, Integer>)countSink.collectedTuples.get(2);

    log.debug(String.format("\nBenchmark sums for %d key/val pairs", numTuples * 3));

    log.debug(String.format("\nCounts were (%d,%d,%d) in random order, got(%d,%d,%d);",
                            numTuples, numTuples, numTuples,
                            c1.getValue().intValue(), c2.getValue().intValue(), c3.getValue().intValue()));
  }
}
