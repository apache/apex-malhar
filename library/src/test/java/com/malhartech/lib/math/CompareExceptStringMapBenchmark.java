/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestCountAndLastTupleSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.CompareExcept}<p>
 *
 */
public class CompareExceptStringMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(CompareExceptStringMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessingSchema()
  {
    CompareExceptStringMap<String> oper = new CompareExceptStringMap<String>();
    TestCountAndLastTupleSink compareSink = new TestCountAndLastTupleSink();
    TestCountAndLastTupleSink exceptSink = new TestCountAndLastTupleSink();
    oper.compare.setSink(compareSink);
    oper.except.setSink(exceptSink);

    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    int numTuples = 50000000;
    HashMap<String, String> input1 = new HashMap<String, String>();
    HashMap<String, String> input2 = new HashMap<String, String>();

    input1.put("a", "2");
    input1.put("b", "20");
    input1.put("c", "1000");
    input2.put("a", "3");
    input2.put("b", "32");
    input2.put("c", "23");

    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmark for %d tuples", numTuples * 2));
  }
}