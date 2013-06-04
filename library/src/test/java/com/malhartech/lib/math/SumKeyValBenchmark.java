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
 * Performance tests for {@link com.malhartech.lib.math.SumKeyVal. }<p>
 *
 */
public class SumKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(SumKeyValBenchmark.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing() throws InterruptedException
  {
    SumKeyVal<String, Double> oper = new SumKeyVal<String, Double>();
    oper.setType(Double.class);
    TestSink sumSink = new TestSink();
    oper.sum.setSink(sumSink);

    int numTuples = 100000000;
    oper.beginWindow(0);

    for (int i = 0; i < numTuples; i++) {
      oper.data.process(new KeyValPair("a", 2.0));
      oper.data.process(new KeyValPair("b", 20.0));
      oper.data.process(new KeyValPair("c", 10.0));
    }
    oper.endWindow();

    KeyValPair<String, Double> sum1 = (KeyValPair<String, Double>)sumSink.collectedTuples.get(0);
    KeyValPair<String, Double> sum2 = (KeyValPair<String, Double>)sumSink.collectedTuples.get(1);
    KeyValPair<String, Double> sum3 = (KeyValPair<String, Double>)sumSink.collectedTuples.get(2);

    log.debug(String.format("\nBenchmark sums for %d key/val pairs", numTuples * 3));

    log.debug(String.format("\nFor sum expected (%d,%d,%d) in random order, got(%.1f,%.1f,%.1f);",
                            2 * numTuples, 20 * numTuples, 10 * numTuples,
                            sum1.getValue(), sum2.getValue(), sum3.getValue()));
  }
}
