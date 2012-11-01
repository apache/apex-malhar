/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Sink;
import com.malhartech.dag.TestCountAndLastTupleSink;
import com.malhartech.dag.TestSink;
import com.malhartech.dag.Tuple;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.Min}<p>
 *
 */
public class MinBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MinBenchmark.class);

  /**
   * Test functional logic
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws InterruptedException
  {
    testSchemaNodeProcessing(new Min<String, Integer>(), "integer"); // 8million/s
    testSchemaNodeProcessing(new Min<String, Double>(), "double"); // 8 million/s
    testSchemaNodeProcessing(new Min<String, Long>(), "long"); // 8 million/s
    testSchemaNodeProcessing(new Min<String, Short>(), "short"); // 8 million/s
    testSchemaNodeProcessing(new Min<String, Float>(), "float"); // 8 million/s
  }

  /**
   * Test oper logic emits correct results for each schema
   */
  public void testSchemaNodeProcessing(Min oper, String type) throws InterruptedException
  {
    TestCountAndLastTupleSink minSink = new TestCountAndLastTupleSink();
    oper.min.setSink(minSink);
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null));

    oper.beginWindow();
    int numtuples = 100000000;
    // For benchmark do -> numtuples = numtuples * 100;
    HashMap<String, Number> tuple = new HashMap<String, Number>();
    if (type.equals("integer")) {
      for (int i = 0; i < numtuples; i++) {
        tuple.put("a", new Integer(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("double")) {
      for (int i = 0; i < numtuples; i++) {
        tuple.put("a", new Double(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("long")) {
      for (int i = 0; i < numtuples; i++) {
        tuple.put("a", new Long(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("short")) {
      for (int j = 0; j < numtuples / 10000; j++) {
        for (short i = 0; i < 10000; i++) {
          tuple.put("a", new Short(i));
          oper.data.process(tuple);
        }
      }
    }
    else if (type.equals("float")) {
      for (int i = 0; i < numtuples; i++) {
        tuple.put("a", new Float(i));
        oper.data.process(tuple);
      }
    }
    oper.endWindow();

    HashMap<String, Number> shash = (HashMap<String, Number>) minSink.tuple;
    Number val = shash.get("a");
    log.debug(String.format("\nBenchmark total for %d tuples; expected 0.0, got %f", numtuples, val));
  }
}
