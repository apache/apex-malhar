/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestRange {
    private static Logger LOG = LoggerFactory.getLogger(Range.class);

  class TestSink implements Sink
  {
    int low = -1;
    int high = -1;

    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        HashMap<String, Object> tuple = (HashMap<String, Object>)payload;
        for (Map.Entry<String, Object> e: tuple.entrySet()) {
          ArrayList<Number> alist = (ArrayList<Number>)e.getValue();
          high = alist.get(0).intValue();
          low = alist.get(1).intValue();
        }
      }
    }
  }

    /**
     * Test configuration and parameter validation of the node
     */
    @Test
    public void testNodeValidation() {

        ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());

        Range node = new Range();
       // Insert tests for expected failure and success here
        node.myValidation(conf);
    }

    /**
     * Test functional logic
     */
    @Test
    public void testNodeProcessing() {
      testSchemaNodeProcessing("integer"); // 8million/s
      testSchemaNodeProcessing("double"); // 8 million/s
      testSchemaNodeProcessing("long"); // 8 million/s
      testSchemaNodeProcessing("short"); // 8 million/s
      testSchemaNodeProcessing("float"); // 8 million/s
    }

    /**
     * Test node logic emits correct results for each schema
     */
    public void testSchemaNodeProcessing(String type) {

      Range node = new Range();

      TestSink rangeSink = new TestSink();
      node.connect(Range.OPORT_RANGE, rangeSink);

      HashMap<String, Double> input = new HashMap<String, Double>();
      Sink outSink = node.connect(Range.IPORT_DATA, node);

    ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
    conf.set(Range.KEY_SCHEMA, type);
    node.setup(conf);

      // do node.setup
      HashMap<String, Number> tuple;
      int numtuples = 1000000;
      // For benchmark do -> numtuples = numtuples * 100;
      if (type.equals("integer")) {
        for (int i = 0; i < numtuples; i++) {
          tuple = new HashMap<String, Number>();
          tuple.put("a", new Integer(i));
          node.process(tuple);
        }
      }
      else if (type.equals("double")) {
        for (int i = 0; i < numtuples; i++) {
          tuple = new HashMap<String, Number>();
          tuple.put("a", new Double(i));
          node.process(tuple);
        }
      }
      else if (type.equals("long")) {
        for (int i = 0; i < numtuples; i++) {
          tuple = new HashMap<String, Number>();
          tuple.put("a", new Long(i));
          node.process(tuple);
        }
      }
      else if (type.equals("short")) {
        // cannot cross 64K
        int count = numtuples/1000;
        for (int j = 0; j < count; j++) {
          for (short i = 0; i < 1000; i++) {
            tuple = new HashMap<String, Number>();
            tuple.put("a", new Short(i));
            node.process(tuple);
          }
        }
      }
       else if (type.equals("float")) {
        for (int i = 0; i < numtuples; i++) {
          tuple = new HashMap<String, Number>();
          tuple.put("a", new Float(i));
          node.process(tuple);
        }
      }
      node.endWindow();
      LOG.debug(String.format("\n****************************\nThe high is %d, and low is %d from %d tuples\n*************************\n",
                              rangeSink.high, rangeSink.low, numtuples));
    }
}
