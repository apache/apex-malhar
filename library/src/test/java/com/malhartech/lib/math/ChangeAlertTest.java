/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.ChangeAlert}<p>
 *
 */
public class ChangeAlertTest
{
  private static Logger log = LoggerFactory.getLogger(ChangeAlertTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeAlert<String, Integer>());
    testNodeProcessingSchema(new ChangeAlert<String, Double>());
    testNodeProcessingSchema(new ChangeAlert<String, Float>());
    testNodeProcessingSchema(new ChangeAlert<String, Short>());
    testNodeProcessingSchema(new ChangeAlert<String, Long>());
  }

  public void testNodeProcessingSchema(ChangeAlert oper)
  {
    TestSink alertSink = new TestSink();

    oper.alert.setSink(alertSink);
    oper.setPercentThreshold(5);

    oper.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 200);
    input.put("b", 10);
    input.put("c", 100);
    oper.data.process(input);

    input.clear();
    input.put("a", 203);
    input.put("b", 12);
    input.put("c", 101);
    oper.data.process(input);

    input.clear();
    input.put("a", 210);
    input.put("b", 12);
    input.put("c", 102);
    oper.data.process(input);

    input.clear();
    input.put("a", 231);
    input.put("b", 18);
    input.put("c", 103);
    oper.data.process(input);
    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 3, alertSink.collectedTuples.size());

    double aval = 0;
    double bval = 0;
    log.debug("\nLogging tuples");
    for (Object o: alertSink.collectedTuples) {
      HashMap<String, HashMap<Number, Double>> map = (HashMap<String, HashMap<Number, Double>>)o;
      Assert.assertEquals("map size", 1, map.size());
      log.debug(o.toString());
      HashMap<Number, Double> vmap = map.get("a");
      if (vmap != null) {
        aval += vmap.get(231).doubleValue();
      }
      vmap = map.get("b");
      if (vmap != null) {
        if (vmap.get(12) != null) {
          bval += vmap.get(12).doubleValue();
        }
        else {
          bval += vmap.get(18).doubleValue();
        }
      }
    }
    Assert.assertEquals("change in a", 10.0, aval);
    Assert.assertEquals("change in a", 70.0, bval);
  }
}