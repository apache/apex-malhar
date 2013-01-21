/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.RangeMap}<p>
 *
 */
public class RangeMapTest
{
  private static Logger log = LoggerFactory.getLogger(RangeMapTest.class);

  class TestSink implements Sink
  {
    double low = -1;
    double high = -1;

    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        HashMap<String, Object> tuple = (HashMap<String, Object>)payload;
        for (Map.Entry<String, Object> e: tuple.entrySet()) {
          ArrayList<Number> alist = (ArrayList<Number>)e.getValue();
          high = alist.get(0).doubleValue();
          low = alist.get(1).doubleValue();
        }
      }
    }
  }

  /**
   * Test functional logic
   */
  @Test
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new RangeMap<String, Integer>(), "integer"); // 8million/s
    testSchemaNodeProcessing(new RangeMap<String, Double>(), "double"); // 8 million/s
    testSchemaNodeProcessing(new RangeMap<String, Long>(), "long"); // 8 million/s
    testSchemaNodeProcessing(new RangeMap<String, Short>(), "short"); // 8 million/s
    testSchemaNodeProcessing(new RangeMap<String, Float>(), "float"); // 8 million/s
  }

  /**
   * Test node logic emits correct results for each schema
   */
  public void testSchemaNodeProcessing(RangeMap node, String type)
  {
    TestSink rangeSink = new TestSink();
    node.range.setSink(rangeSink);

    HashMap<String, Number> input = new HashMap<String, Number>();
    int numtuples = 1000;
    // For benchmark do -> numtuples = numtuples * 100;
    if (type.equals("integer")) {
      HashMap<String, Integer> tuple;
      for (int i = -10; i < numtuples; i++) {
        tuple = new HashMap<String, Integer>();
        tuple.put("a", new Integer(i));
        node.data.process(tuple);
      }
    }
    else if (type.equals("double")) {
      HashMap<String, Double> tuple;
      for (int i = -10; i < numtuples; i++) {
        tuple = new HashMap<String, Double>();
        tuple.put("a", new Double(i));
        node.data.process(tuple);
      }
    }
    else if (type.equals("long")) {
      HashMap<String, Long> tuple;
      for (int i = -10; i < numtuples; i++) {
        tuple = new HashMap<String, Long>();
        tuple.put("a", new Long(i));
        node.data.process(tuple);
      }
    }
    else if (type.equals("short")) {
      HashMap<String, Short> tuple;
      for (short i = -10; i < 1000; i++) {
        tuple = new HashMap<String, Short>();
        tuple.put("a", new Short(i));
        node.data.process(tuple);
      }
    }
    else if (type.equals("float")) {
      HashMap<String, Float> tuple;
      for (int i = -10; i < numtuples; i++) {
        tuple = new HashMap<String, Float>();
        tuple.put("a", new Float(i));
        node.data.process(tuple);
      }
    }
    node.endWindow();
    Assert.assertEquals("high was ", new Double(999.0), rangeSink.high);
    Assert.assertEquals("low was ", new Double(-10.0), rangeSink.low);
    log.debug(String.format("\nTested %d tuples", numtuples));
  }
}
