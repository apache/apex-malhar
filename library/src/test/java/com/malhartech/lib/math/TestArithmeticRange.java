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
public class TestArithmeticRange {
    private static Logger LOG = LoggerFactory.getLogger(ArithmeticRange.class);

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
          ArrayList<Integer> alist = (ArrayList<Integer>)e.getValue();
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

        ArithmeticRange node = new ArithmeticRange();
       // Insert tests for expected failure and success here
        node.myValidation(conf);
    }

    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() {

      ArithmeticRange node = new ArithmeticRange();

      TestSink rangeSink = new TestSink();
      node.connect(ArithmeticRange.OPORT_RANGE, rangeSink);

      HashMap<String, Double> input = new HashMap<String, Double>();
      Sink outSink = node.connect(ArithmeticRange.IPORT_DATA, node);

    ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
    conf.set(ArithmeticRange.KEY_SCHEMA, "integer");
    node.setup(conf);

    HashMap<String, Integer> tuple;
      // do node.setup
      int numtuples = 100000000;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Integer>();
        tuple.put("a", new Integer(i));
        node.process(tuple);
      }
      node.endWindow();
      LOG.debug(String.format("The high is %d, and low is %d", rangeSink.high, rangeSink.low));
    }
}
