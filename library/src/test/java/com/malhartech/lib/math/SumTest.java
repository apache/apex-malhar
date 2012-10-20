/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
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
public class SumTest {
    private static Logger LOG = LoggerFactory.getLogger(Sum.class);

    class TestSink implements Sink {

        List<Object> collectedTuples = new ArrayList<Object>();

      @Override
      public void process(Object payload)
      {
        if (payload instanceof Tuple) {
        }
        else {
          collectedTuples.add(payload);
        }
      }
    }

    /**
     * Test configuration and parameter validation of the node
     */
    @Test
    public void testNodeValidation() {

        OperatorConfiguration conf = new OperatorConfiguration("mynode", new HashMap<String, String>());

        Sum node = new Sum();

        // Insert tests for expected failure and success here
        node.myValidation(conf);
    }


    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() {
      testNodeSchemaProcessing(true, false);
      testNodeSchemaProcessing(true, true);
      testNodeSchemaProcessing(false, true);
    }

  public void testNodeSchemaProcessing(boolean sum, boolean count)
  {

    Sum node = new Sum();
    TestSink sumSink = new TestSink();
    TestSink countSink = new TestSink();
    if (sum) {
      node.connect(Sum.OPORT_SUM, sumSink);
    }
    if (count) {
      node.connect(Sum.OPORT_COUNT, countSink);
    }
    OperatorConfiguration conf = new OperatorConfiguration("testsumnode", new HashMap<String, String>());
    node.setup(conf);
    node.beginWindow(); //

    HashMap<String, Double> input = new HashMap<String, Double>();

    input.put("a", 2.0);
    input.put("b", 20.0);
    input.put("c", 1000.0);
    node.process(input);
    input.clear();
    input.put("a", 1.0);
    node.process(input);
    input.clear();
    input.put("a", 10.0);
    input.put("b", 5.0);
    node.process(input);
    input.clear();
    input.put("d", 55.0);
    input.put("b", 12.0);
    node.process(input);
    input.clear();
    input.put("d", 22.0);
    node.process(input);
    input.clear();
    input.put("d", 14.2);
    node.process(input);
    input.clear();

    // Mix integers and doubles
    HashMap<String, Integer> inputi = new HashMap<String, Integer>();
    inputi.put("d", 46);
    inputi.put("e", 2);
    node.process(inputi);
    inputi.clear();
    inputi.put("a", 23);
    inputi.put("d", 4);
    node.process(inputi);
    inputi.clear();

    node.endWindow(); //

    if (sum) {
      // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
      Assert.assertEquals("number emitted tuples", 1, sumSink.collectedTuples.size());
      for (Object o: sumSink.collectedTuples) {
        HashMap<String, Object> output = (HashMap<String, Object>)o;
        for (Map.Entry<String, Object> e: output.entrySet()) {
          Double val = (Double)e.getValue();
          if (e.getKey().equals("a")) {
            Assert.assertEquals("emitted value for 'a' was ", new Double(36), val);
          }
          else if (e.getKey().equals("b")) {
            Assert.assertEquals("emitted tuple for 'b' was ", new Double(37), val);
          }
          else if (e.getKey().equals("c")) {
            Assert.assertEquals("emitted tuple for 'c' was ", new Double(1000), val);
          }
          else if (e.getKey().equals("d")) {
            Assert.assertEquals("emitted tuple for 'd' was ", new Double(141.2), val);
          }
          else if (e.getKey().equals("e")) {
            Assert.assertEquals("emitted tuple for 'e' was ", new Double(2), val);
          }
        }
      }
    }

    if (count) {
      // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
      Assert.assertEquals("number emitted tuples", 1, countSink.collectedTuples.size());
      for (Object o: countSink.collectedTuples) {
        HashMap<String, Object> output = (HashMap<String, Object>)o;
        for (Map.Entry<String, Object> e: output.entrySet()) {
          Integer val = (Integer) e.getValue();
          if (e.getKey().equals("a")) {
            Assert.assertEquals("emitted value for 'a' was ", 4, val.intValue());
          }
          else if (e.getKey().equals("b")) {
            Assert.assertEquals("emitted tuple for 'b' was ", 3, val.intValue());
          }
          else if (e.getKey().equals("c")) {
            Assert.assertEquals("emitted tuple for 'c' was ", 1, val.intValue());
          }
          else if (e.getKey().equals("d")) {
            Assert.assertEquals("emitted tuple for 'd' was ", 5, val.intValue());
          }
          else if (e.getKey().equals("e")) {
            Assert.assertEquals("emitted tuple for 'e' was ", 1, val.intValue());
          }
        }
      }
    }

  }
}
