/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.math.ChangeMap;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.ChangeMap}. <p>
 *
 */
public class ChangeMapTest
{
  private static Logger log = LoggerFactory.getLogger(ChangeMapTest.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeMap<String, Integer>());
    testNodeProcessingSchema(new ChangeMap<String, Double>());
    testNodeProcessingSchema(new ChangeMap<String, Float>());
    testNodeProcessingSchema(new ChangeMap<String, Short>());
    testNodeProcessingSchema(new ChangeMap<String, Long>());
  }

  public <V extends Number> void testNodeProcessingSchema(ChangeMap<String, V> oper)
  {
    TestSink changeSink = new TestSink();
    TestSink percentSink = new TestSink();

    oper.change.setSink(changeSink);
    oper.percent.setSink(percentSink);

    oper.beginWindow(0);
    HashMap<String, V> input = new HashMap<String, V>();
    input.put("a", oper.getValue(2));
    input.put("b", oper.getValue(10));
    input.put("c", oper.getValue(100));
    oper.base.process(input);

    input.clear();
    input.put("a", oper.getValue(3));
    input.put("b", oper.getValue(2));
    input.put("c", oper.getValue(4));
    oper.data.process(input);

    input.clear();
    input.put("a", oper.getValue(4));
    input.put("b", oper.getValue(19));
    input.put("c", oper.getValue(150));
    oper.data.process(input);

    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 6, changeSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 6, percentSink.collectedTuples.size());

    double aval = 0;
    double bval = 0;
    double cval = 0;
    log.debug("\nLogging tuples");
    for (Object o: changeSink.collectedTuples) {
      @SuppressWarnings("unchecked")
      HashMap<String, Number> map = (HashMap<String, Number>)o;
      Assert.assertEquals("map size", 1, map.size());
      Number anum = map.get("a");
      Number bnum = map.get("b");
      Number cnum = map.get("c");
      if (anum != null) {
        aval += anum.doubleValue();
      }
      if (bnum != null) {
        bval += bnum.doubleValue();
      }
      if (cnum != null) {
        cval += cnum.doubleValue();
      }
    }
    Assert.assertEquals("change in a", 3.0, aval);
    Assert.assertEquals("change in a", 1.0, bval);
    Assert.assertEquals("change in a", -46.0, cval);

    aval = 0.0;
    bval = 0.0;
    cval = 0.0;

    for (Object o: percentSink.collectedTuples) {
      @SuppressWarnings("unchecked")
      HashMap<String, Number> map = (HashMap<String, Number>)o;
      Assert.assertEquals("map size", 1, map.size());
      Number anum = map.get("a");
      Number bnum = map.get("b");
      Number cnum = map.get("c");
      if (anum != null) {
        aval += anum.doubleValue();
      }
      if (bnum != null) {
        bval += bnum.doubleValue();
      }
      if (cnum != null) {
        cval += cnum.doubleValue();
      }
    }
    Assert.assertEquals("change in a", 150.0, aval);
    Assert.assertEquals("change in a", 10.0, bval);
    Assert.assertEquals("change in a", -46.0, cval);
  }
}