/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.dag.TestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.FirstN} <p>
 *
 */
public class FirstNBenchmark
{
  private static Logger log = LoggerFactory.getLogger(FirstNBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new FirstN<String, Integer>());
    testNodeProcessingSchema(new FirstN<String, Double>());
    testNodeProcessingSchema(new FirstN<String, Float>());
    testNodeProcessingSchema(new FirstN<String, Short>());
    testNodeProcessingSchema(new FirstN<String, Long>());
  }

  public void testNodeProcessingSchema(FirstN oper)
  {
    //FirstN<String,Float> aoper = new FirstN<String,Float>();
    TestSink<HashMap<String, Number>> sortSink = new TestSink<HashMap<String, Number>>();
    oper.first.setSink(sortSink);
    //oper.setN(3);

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      input.put("a", 2);
      oper.data.process(input);

      input.clear();
      input.put("a", 20);
      oper.data.process(input);

      input.clear();
      input.put("a", 1000);
      oper.data.process(input);

      input.clear();
      input.put("a", 5);
      oper.data.process(input);

      input.clear();
      input.put("a", 20);
      input.put("b", 33);
      oper.data.process(input);

      input.clear();
      input.put("a", 33);
      input.put("b", 34);
      oper.data.process(input);

      input.clear();
      input.put("b", 34);
      input.put("a", 1001);
      oper.data.process(input);

      input.clear();
      input.put("b", 6);
      input.put("a", 1);
      oper.data.process(input);
      input.clear();
      input.put("c", 9);
      oper.data.process(input);
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 9, sortSink.collectedTuples.size());
    int aval = 0;
    int bval = 0;
    int cval = 0;
    for (Object o: sortSink.collectedTuples) {
      for (Map.Entry<String, Number> e: ((HashMap<String, Number>)o).entrySet()) {
        if (e.getKey().equals("a")) {
          aval += e.getValue().intValue();
        }
        else if (e.getKey().equals("b")) {
          bval += e.getValue().intValue();
        }
        else if (e.getKey().equals("c")) {
          cval += e.getValue().intValue();
        }
      }
    }
    Assert.assertEquals("Value of \"a\" was ", 1022, aval);
    Assert.assertEquals("Value of \"a\" was ", 101, bval);
    Assert.assertEquals("Value of \"a\" was ", 27, cval);
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 9));
  }
}
