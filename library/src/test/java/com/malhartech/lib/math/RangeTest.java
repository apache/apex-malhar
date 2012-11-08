/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.Range}<p>
 *
 */
public class RangeTest {
    private static Logger LOG = LoggerFactory.getLogger(RangeTest.class);

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
          ArrayList<Number> alist = (ArrayList<Number>) e.getValue();
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
    public void testNodeProcessing() {
      testSchemaNodeProcessing(new Range<String,Integer>(), "integer"); // 8million/s
      testSchemaNodeProcessing(new Range<String,Double>(), "double"); // 8 million/s
      testSchemaNodeProcessing(new Range<String,Long>(), "long"); // 8 million/s
      testSchemaNodeProcessing(new Range<String,Short>(),"short"); // 8 million/s
      testSchemaNodeProcessing(new Range<String,Float>(),"float"); // 8 million/s
    }

    /**
     * Test node logic emits correct results for each schema
     */
    public void testSchemaNodeProcessing(Range node, String type)
    {
      TestSink rangeSink = new TestSink();
      node.range.setSink(rangeSink);
      node.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));

      HashMap<String, Number> input = new HashMap<String, Number>();
      int numtuples = 1000;
      // For benchmark do -> numtuples = numtuples * 100;
      if (type.equals("integer")) {
        HashMap<String,Integer> tuple;
        for (int i = 0; i < numtuples; i++) {
          tuple = new HashMap<String, Integer>();
          tuple.put("a", new Integer(i));
          node.data.process(tuple);
        }
      }
      else if (type.equals("double")) {
        HashMap<String,Double> tuple;
        for (int i = 0; i < numtuples; i++) {
          tuple = new HashMap<String, Double>();
          tuple.put("a", new Double(i));
          node.data.process(tuple);
        }
      }
      else if (type.equals("long")) {
        HashMap<String,Long> tuple;
        for (int i = 0; i < numtuples; i++) {
          tuple = new HashMap<String, Long>();
          tuple.put("a", new Long(i));
          node.data.process(tuple);
        }
      }
      else if (type.equals("short")) {
        HashMap<String,Short> tuple;
        int count = numtuples/1000; // cannot cross 64K
        for (int j = 0; j < count; j++) {
          for (short i = 0; i < 1000; i++) {
            tuple = new HashMap<String, Short>();
            tuple.put("a", new Short(i));
            node.data.process(tuple);
          }
        }
      }
       else if (type.equals("float")) {
        HashMap<String,Float> tuple;
        for (int i = 0; i < numtuples; i++) {
          tuple = new HashMap<String, Float>();
          tuple.put("a", new Float(i));
          node.data.process(tuple);
        }
      }
      node.endWindow();
      LOG.debug(String.format("\n****************************\nThe high is %f, and low is %f from %d tuples\n*************************\n",
                              rangeSink.high, rangeSink.low, numtuples));
    }
}
