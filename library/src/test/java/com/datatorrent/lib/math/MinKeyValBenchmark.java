/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.MinKeyVal;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import com.datatorrent.lib.util.KeyValPair;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.MinKeyVal}. <p>
 * Current benchmark is 35 million tuples/sec.
 *
 */
public class MinKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MinKeyValBenchmark.class);

  /**
   * Test functional logic
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new MinKeyVal<String, Integer>(), "integer");
    testSchemaNodeProcessing(new MinKeyVal<String, Double>(), "double");
    testSchemaNodeProcessing(new MinKeyVal<String, Long>(), "long");
    testSchemaNodeProcessing(new MinKeyVal<String, Short>(), "short");
    testSchemaNodeProcessing(new MinKeyVal<String, Float>(), "float");
  }

  /**
   * Test operator logic emits correct results for each schema.
   *
   */
  public void testSchemaNodeProcessing(MinKeyVal oper, String type)
  {
    CountAndLastTupleTestSink minSink = new CountAndLastTupleTestSink();
    oper.min.setSink(minSink);

    oper.beginWindow(0);

    int numtuples = 100000000;
    if (type.equals("integer")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", new Integer(i)));
      }
    }
    else if (type.equals("double")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", new Double(i)));
      }
    }
    else if (type.equals("long")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", new Long(i)));
      }
    }
    else if (type.equals("short")) {
      int count = numtuples / 1000; // cannot cross 64K
      for (int i = 0; i < count; i++) {
        for (short j = 0; j < 1000; j++) {
          oper.data.process(new KeyValPair("a", new Short(j)));
        }
      }
    }
    else if (type.equals("float")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", new Float(i)));
      }
    }

    oper.endWindow();
    log.debug(String.format("\nBenchmark total for %d tuples", numtuples));
  }
}
