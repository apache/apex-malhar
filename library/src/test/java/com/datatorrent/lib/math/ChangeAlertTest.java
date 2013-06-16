/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.math.ChangeAlert;
import com.datatorrent.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.ChangeAlert}. <p>
 *
 */
public class ChangeAlertTest
{
  private static Logger log = LoggerFactory.getLogger(ChangeAlertTest.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeAlert<Integer>());
    testNodeProcessingSchema(new ChangeAlert<Double>());
    testNodeProcessingSchema(new ChangeAlert<Float>());
    testNodeProcessingSchema(new ChangeAlert<Short>());
    testNodeProcessingSchema(new ChangeAlert<Long>());
  }

  public <V extends Number> void testNodeProcessingSchema(ChangeAlert<V> oper)
  {
    TestSink alertSink = new TestSink();

    oper.alert.setSink(alertSink);
    oper.setPercentThreshold(5);

    oper.beginWindow(0);
    oper.data.process(oper.getValue(10));
    oper.data.process(oper.getValue(12)); // alert
    oper.data.process(oper.getValue(12));
    oper.data.process(oper.getValue(18)); // alert
    oper.data.process(oper.getValue(0));  // alert
    oper.data.process(oper.getValue(20)); // this will not alert
    oper.data.process(oper.getValue(30)); // alert

    oper.endWindow();

    // One for a, Two for b
    Assert.assertEquals("number emitted tuples", 4, alertSink.collectedTuples.size());

    double aval = 0;
    log.debug("\nLogging tuples");
    for (Object o: alertSink.collectedTuples) {
      @SuppressWarnings("unchecked")
      KeyValPair<Number, Double> map = (KeyValPair<Number, Double>)o;
      log.debug(o.toString());
      aval += map.getValue().doubleValue();
    }
    Assert.assertEquals("change in a", 220.0, aval);
  }
}