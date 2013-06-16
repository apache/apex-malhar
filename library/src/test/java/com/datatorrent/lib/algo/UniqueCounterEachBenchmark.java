/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.algo.UniqueCounter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.UniqueCounterEach}<p>
 *
 */
public class UniqueCounterEachBenchmark
{
  private static Logger log = LoggerFactory.getLogger(UniqueCounterEachBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    UniqueCounter<String> oper = new UniqueCounter<String>();
    TestSink sink = new TestSink();
    oper.count.setSink(sink);

    String atuple = "a";
    String btuple = "b";
    String ctuple = "c";
    String dtuple = "d";
    String etuple = "e";

    int numTuples = 100000000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(atuple);
      if (i % 2 == 0) {
        oper.data.process(btuple);
      }
      if (i % 3 == 0) {
        oper.data.process(ctuple);
      }
      if (i % 5 == 0) {
        oper.data.process(dtuple);
      }
      if (i % 10 == 0) {
        oper.data.process(etuple);
      }
    }
    oper.endWindow();
    int count = 0;
    for (Object e : sink.collectedTuples) {
      for (Map.Entry<String, Integer> val: ((HashMap<String,Integer>)e).entrySet()) {
        count += val.getValue().intValue();
      }
    }
    log.debug(String.format("\nBenchmarked %d tuples", count));
  }
}
