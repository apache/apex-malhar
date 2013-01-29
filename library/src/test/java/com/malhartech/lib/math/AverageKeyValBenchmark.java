/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestSink;
import com.malhartech.lib.util.KeyValPair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.AverageKeyVal}. <p>
 * Current benchmark 13 million tuples per second.
 *
 */
public class AverageKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(AverageKeyValBenchmark.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing() throws InterruptedException
  {
    AverageKeyVal<String> oper = new AverageKeyVal<String>();
    TestSink<KeyValPair<String, Double>> averageSink = new TestSink<KeyValPair<String, Double>>();
    oper.doubleAverage.setSink(averageSink);

    int numTuples = 100000000;
    oper.beginWindow(0);

    for (int i = 0; i < numTuples; i++) {
      oper.data.process(new KeyValPair("a", 2.0));
      oper.data.process(new KeyValPair("b", 20.0));
      oper.data.process(new KeyValPair("c", 10.0));
    }
    oper.endWindow();

    KeyValPair<String, Double> ave1 = (KeyValPair<String, Double>) averageSink.collectedTuples.get(0);
    KeyValPair<String, Double> ave2 = (KeyValPair<String, Double>) averageSink.collectedTuples.get(1);
    KeyValPair<String, Double> ave3 = (KeyValPair<String, Double>) averageSink.collectedTuples.get(2);

    log.debug(String.format("\nBenchmark sums for %d key/val pairs", numTuples * 3));

    log.debug(String.format("\nFor average expected(2,20,10) in random order, got(%d,%d,%d);",
                            ave1.getValue().intValue(), ave2.getValue().intValue(), ave3.getValue().intValue()));
  }
}
