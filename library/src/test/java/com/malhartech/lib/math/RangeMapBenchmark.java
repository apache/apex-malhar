/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Sink;
import com.malhartech.lib.util.HighLow;
import com.malhartech.tuple.Tuple;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.RangeMap}<p>
 *
 */
public class RangeMapBenchmark
{
  private static Logger LOG = LoggerFactory.getLogger(RangeMapBenchmark.class);

  class TestSink implements Sink
  {
    double low = -1;
    double high = -1;

    @Override
    public void put(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        HashMap<String, Object> tuple = (HashMap<String, Object>)payload;
        for (Map.Entry<String, Object> e: tuple.entrySet()) {
          HighLow hl = (HighLow)e.getValue();
          high = hl.getHigh().doubleValue();
          low = hl.getLow().doubleValue();
        }
      }
    }

    @Override
    public int getCount(boolean reset)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  /**
   * Test functional logic
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
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
    int numtuples = 100000000;
    // For benchmark do -> numtuples = numtuples * 100;
    if (type.equals("integer")) {
      HashMap<String, Integer> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Integer>();
        tuple.put("a", new Integer(i));
        node.data.process(tuple);
      }
    }
    else if (type.equals("double")) {
      HashMap<String, Double> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Double>();
        tuple.put("a", new Double(i));
        node.data.process(tuple);
      }
    }
    else if (type.equals("long")) {
      HashMap<String, Long> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Long>();
        tuple.put("a", new Long(i));
        node.data.process(tuple);
      }
    }
    else if (type.equals("short")) {
      HashMap<String, Short> tuple;
      int count = numtuples / 1000; // cannot cross 64K
      for (int j = 0; j < count; j++) {
        for (short i = 0; i < 1000; i++) {
          tuple = new HashMap<String, Short>();
          tuple.put("a", new Short(i));
          node.data.process(tuple);
        }
      }
    }
    else if (type.equals("float")) {
      HashMap<String, Float> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Float>();
        tuple.put("a", new Float(i));
        node.data.process(tuple);
      }
    }
    node.endWindow();
    LOG.debug(String.format("\n****************************\nThe high is %f, and low is %f from %d tuples of type %s\n*************************\n",
                            rangeSink.high, rangeSink.low, numtuples, type));
  }
}
