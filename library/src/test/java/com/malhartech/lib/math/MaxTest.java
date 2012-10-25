/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestCountAndLastTupleSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MaxTest
{
  private static Logger log = LoggerFactory.getLogger(MaxTest.class);

  /**
   * Test functional logic
   */
  @Test
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
    oper.setup(new OperatorConfiguration());

    oper.beginWindow();

    HashMap<String, Number> input = new HashMap<String, Number>();
    int numtuples = 10000;
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
      for (short j = 0; j < count; j++) {
        tuple = new HashMap<String, Short>();
        tuple.put("a", new Short(j));
        oper.data.process(tuple);

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

    Assert.assertEquals("number emitted tuples", 1, maxSink.count);
    Number val = ((HashMap<String, Number>)maxSink.tuple).get("a");
    if (type.equals("short")) {
      Assert.assertEquals("emitted min value was ", new Double(numtuples / 1000 - 1), val);
    }
    else {
      Assert.assertEquals("emitted min value was ", new Double(numtuples - 1), val);
    }
  }
}
