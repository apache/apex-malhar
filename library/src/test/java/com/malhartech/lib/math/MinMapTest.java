/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestCountAndLastTupleSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.MinMap}<p>
 *
 */
public class MinMapTest
{
  private static Logger log = LoggerFactory.getLogger(MinMapTest.class);

  /**
   * Test functional logic
   */
  @Test
  public void testNodeProcessing() throws InterruptedException
  {
    testSchemaNodeProcessing(new MinMap<String, Integer>(), "integer"); // 8million/s
    testSchemaNodeProcessing(new MinMap<String, Double>(), "double"); // 8 million/s
    testSchemaNodeProcessing(new MinMap<String, Long>(), "long"); // 8 million/s
    testSchemaNodeProcessing(new MinMap<String, Short>(), "short"); // 8 million/s
    testSchemaNodeProcessing(new MinMap<String, Float>(), "float"); // 8 million/s
  }

  /**
   * Test oper logic emits correct results for each schema
   */
  public void testSchemaNodeProcessing(MinMap oper, String type) throws InterruptedException
  {
    TestCountAndLastTupleSink minSink = new TestCountAndLastTupleSink();
    oper.min.setSink(minSink);

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    int numtuples = 100;
    // For benchmark do -> numtuples = numtuples * 100;
    if (type.equals("integer")) {
      HashMap<String, Integer> tuple = new HashMap<String, Integer>();
      for (int i = 0; i < numtuples; i++) {
        tuple.put("a", new Integer(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("double")) {
      HashMap<String, Double> tuple = new HashMap<String, Double>();
      for (int i = 0; i < numtuples; i++) {
        tuple.put("a", new Double(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("long")) {
      HashMap<String, Long> tuple = new HashMap<String, Long>();
      for (int i = 0; i < numtuples; i++) {
        tuple.put("a", new Long(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("short")) {
      HashMap<String, Short> tuple = new HashMap<String, Short>();
      for (short i = 0; i < numtuples; i++) {
        tuple.put("a", new Short(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("float")) {
      HashMap<String, Float> tuple = new HashMap<String, Float>();
      for (int i = 0; i < numtuples; i++) {
        tuple.put("a", new Float(i));
        oper.data.process(tuple);
      }
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, minSink.count);
    HashMap<String, Number> shash = (HashMap<String, Number>) minSink.tuple;
    Number val = shash.get("a");
    Assert.assertEquals("number emitted tuples", 1, shash.size());
    Assert.assertEquals("emitted min value was ", new Double(0.0), val);
  }
}
