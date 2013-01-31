/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestSink;
import com.malhartech.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.ChangeAlertKeyVal}. <p>
 *
 */
public class ChangeAlertKeyValTest
{
  private static Logger log = LoggerFactory.getLogger(ChangeAlertKeyValTest.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Integer>());
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Double>());
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Float>());
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Short>());
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Long>());
  }

  public <V extends Number> void testNodeProcessingSchema(ChangeAlertKeyVal<String, V> oper)
  {
    TestSink<KeyValPair<String, KeyValPair<V, Double>>> alertSink = new TestSink<KeyValPair<String, KeyValPair<V, Double>>>();

    oper.alert.setSink(alertSink);
    oper.setPercentThreshold(5);

    oper.beginWindow(0);
    oper.data.process(new KeyValPair<String, V>("a", oper.getValue(200)));
    oper.data.process(new KeyValPair<String, V>("b", oper.getValue(10)));
    oper.data.process(new KeyValPair<String, V>("c", oper.getValue(100)));

    oper.data.process(new KeyValPair<String, V>("a", oper.getValue(203)));
    oper.data.process(new KeyValPair<String, V>("b", oper.getValue(12)));
    oper.data.process(new KeyValPair<String, V>("c", oper.getValue(101)));

    oper.data.process(new KeyValPair<String, V>("a", oper.getValue(210)));
    oper.data.process(new KeyValPair<String, V>("b", oper.getValue(12)));
    oper.data.process(new KeyValPair<String, V>("c", oper.getValue(102)));

    oper.data.process(new KeyValPair<String, V>("a", oper.getValue(231)));
    oper.data.process(new KeyValPair<String, V>("b", oper.getValue(18)));
    oper.data.process(new KeyValPair<String, V>("c", oper.getValue(103)));
    oper.endWindow();

    // One for a, Two for b
    Assert.assertEquals("number emitted tuples", 3, alertSink.collectedTuples.size());

    double aval = 0;
    double bval = 0;
    log.debug("\nLogging tuples");
    for (Object o: alertSink.collectedTuples) {
      @SuppressWarnings("unchecked")
      KeyValPair<String, KeyValPair<Number, Double>> map = (KeyValPair<String, KeyValPair<Number, Double>>)o;

      log.debug(o.toString());
      if (map.getKey().equals("a")) {
        KeyValPair<Number, Double> vmap = map.getValue();
        if (vmap != null) {
          aval += vmap.getValue().doubleValue();
        }
      }
      else {
        KeyValPair<Number, Double> vmap = map.getValue();
        if (vmap != null) {
          bval += vmap.getValue().doubleValue();
        }
      }
    }
    Assert.assertEquals("change in a", 10.0, aval);
    Assert.assertEquals("change in a", 70.0, bval);
  }
}