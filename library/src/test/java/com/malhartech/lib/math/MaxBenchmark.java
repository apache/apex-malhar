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
 * Performance tests for {@link com.malhartech.lib.math.Max}<p>
 *
 */
public class MaxBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MaxBenchmark.class);

  /**
   * Test functional logic
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new Max<String, Integer>(), "integer"); // 8million/s
    testSchemaNodeProcessing(new Max<String, Double>(), "double"); // 8 million/s
    testSchemaNodeProcessing(new Max<String, Long>(), "long"); // 8 million/s
    testSchemaNodeProcessing(new Max<String, Short>(), "short"); // 8 million/s
    testSchemaNodeProcessing(new Max<String, Float>(), "float"); // 8 million/s
  }

  /**
   * Test oper logic emits correct results for each schema
   */
  public void testSchemaNodeProcessing(Max oper, String type)
  {
    TestCountAndLastTupleSink maxSink = new TestCountAndLastTupleSink();
    oper.max.setSink(maxSink);
    oper.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));

    oper.beginWindow(0);

    HashMap<String, Number> input = new HashMap<String, Number>();
    int numtuples = 100000000;
    // For benchmark do -> numtuples = numtuples * 100;
    if (type.equals("integer")) {
      HashMap<String, Integer> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Integer>();
        tuple.put("a", new Integer(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("double")) {
      HashMap<String, Double> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Double>();
        tuple.put("a", new Double(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("long")) {
      HashMap<String, Long> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Long>();
        tuple.put("a", new Long(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("short")) {
      HashMap<String, Short> tuple;
      int count = numtuples / 1000; // cannot cross 64K
      for (int j = 0; j < count; j++) {
        for (short i = 0; i < 1000; i++) {
          tuple = new HashMap<String, Short>();
          tuple.put("a", new Short(i));
          oper.data.process(tuple);
        }
      }
    }
    else if (type.equals("float")) {
      HashMap<String, Float> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Float>();
        tuple.put("a", new Float(i));
        oper.data.process(tuple);
      }
    }
    oper.endWindow();

    HashMap<String, Number> shash = (HashMap<String, Number>)maxSink.tuple;
    Number val = shash.get("a");
    log.debug(String.format("\nBenchmark total for %d tuples; expected 0.0, got %f", numtuples, val));
  }
}
