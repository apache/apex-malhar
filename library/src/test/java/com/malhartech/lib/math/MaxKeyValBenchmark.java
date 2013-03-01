/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.lib.testbench.CountAndLastTupleTestSink;
import com.malhartech.lib.util.KeyValPair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.MaxKeyVal}. <p>
 * Current benchmark is 35 million tuples/sec.
 *
 */
public class MaxKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MaxKeyValBenchmark.class);

  /**
   * Test functional logic
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new MaxKeyVal<String, Integer>(), "integer");
    testSchemaNodeProcessing(new MaxKeyVal<String, Double>(), "double");
    testSchemaNodeProcessing(new MaxKeyVal<String, Long>(), "long");
    testSchemaNodeProcessing(new MaxKeyVal<String, Short>(), "short");
    testSchemaNodeProcessing(new MaxKeyVal<String, Float>(), "float");
  }

  /**
   * Test operator logic emits correct results for each schema.
   *
   */
  public void testSchemaNodeProcessing(MaxKeyVal oper, String type)
  {
    CountAndLastTupleTestSink maxSink = new CountAndLastTupleTestSink();
    oper.max.setSink(maxSink);

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
