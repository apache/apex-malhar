/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.TestSink;
import com.malhartech.dag.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.Sum}<p>
 *
 */
public class SumBenchmark
{
  private static Logger log = LoggerFactory.getLogger(SumBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing() throws InterruptedException
  {
    Sum<String, Double> oper = new Sum<String, Double>();
    oper.setType(Double.class);
    TestSink sumSink = new TestSink();
    TestSink countSink = new TestSink();
    TestSink averageSink = new TestSink();
    oper.sum.setSink(sumSink);
    oper.count.setSink(countSink);
    oper.average.setSink(averageSink);

    int numTuples = 100000000;
    // Not needed, but still setup is being called as a matter of discipline
    oper.setup(new OperatorConfiguration());
    oper.beginWindow(); //

    HashMap<String, Double> input = new HashMap<String, Double>();

    for (int i = 0; i < numTuples; i++) {
      input.put("a", 2.0);
      input.put("b", 20.0);
      input.put("c", 10.0);
      oper.data.process(input);
    }

    oper.endWindow(); //

    HashMap<String, Double> dhash = (HashMap<String, Double>) sumSink.collectedTuples.get(0);
    HashMap<String, Double> ahash = (HashMap<String, Double>) averageSink.collectedTuples.get(0);
    HashMap<String, Integer> chash = (HashMap<String, Integer>) countSink.collectedTuples.get(0);

    log.debug(String.format("\nBenchmark sums for %d tuples, expected(%d,%d,%d), got(%f,%f,%f);", numTuples,
                                                                                                 2*numTuples, 20*numTuples, 10*numTuples,
                                                                                                 dhash.get("a"), dhash.get("b"), dhash.get("c")));

    log.debug(String.format("\nBenchmark sums for %d tuples, expected(2,20,10), got(%f,%f,%f);", numTuples,
                                                                                                 ahash.get("a"), ahash.get("b"), ahash.get("c")));

    log.debug(String.format("\nBenchmark counts for %d tuples, expected(%d,%d,%d), got(%d,%d,%d);", numTuples,
                                                                                                 2*numTuples, 20*numTuples, 10*numTuples,
                                                                                                 chash.get("a"), chash.get("b"), chash.get("c")));
  }
}
