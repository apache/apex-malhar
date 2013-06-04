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
 * Functional tests for {@link com.malhartech.lib.math.ChangeKeyVal}. <p>
 *
 */
public class ChangeKeyValTest
{
  private static Logger log = LoggerFactory.getLogger(ChangeKeyValTest.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeKeyVal<String, Integer>());
    testNodeProcessingSchema(new ChangeKeyVal<String, Double>());
    testNodeProcessingSchema(new ChangeKeyVal<String, Float>());
    testNodeProcessingSchema(new ChangeKeyVal<String, Short>());
    testNodeProcessingSchema(new ChangeKeyVal<String, Long>());
  }

  /**
   *
   * @param oper
   */
  public <V extends Number> void testNodeProcessingSchema(ChangeKeyVal<String, V> oper)
  {
    TestSink<KeyValPair<String, V>> changeSink = new TestSink<KeyValPair<String, V>>();
    TestSink<KeyValPair<String, Double>> percentSink = new TestSink<KeyValPair<String, Double>>();

    oper.change.setSink(changeSink);
    oper.percent.setSink(percentSink);

    oper.beginWindow(0);
    oper.base.process(new KeyValPair<String, V>("a", oper.getValue(2)));
    oper.base.process(new KeyValPair<String, V>("b", oper.getValue(10)));
    oper.base.process(new KeyValPair<String, V>("c", oper.getValue(100)));

    oper.data.process(new KeyValPair<String, V>("a", oper.getValue(3)));
    oper.data.process(new KeyValPair<String, V>("b", oper.getValue(2)));
    oper.data.process(new KeyValPair<String, V>("c", oper.getValue(4)));

    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 3, changeSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 3, percentSink.collectedTuples.size());

    log.debug("\nLogging tuples");
    for (Object o: changeSink.collectedTuples) {
      @SuppressWarnings("unchecked")
      KeyValPair<String, Number> kv = (KeyValPair<String, Number>)o;
      if (kv.getKey().equals("a")) {
        Assert.assertEquals("change in a ", 1.0, kv.getValue());
      }
      if (kv.getKey().equals("b")) {
        Assert.assertEquals("change in b ", -8.0, kv.getValue());
      }
      if (kv.getKey().equals("c")) {
        Assert.assertEquals("change in c ", -96.0, kv.getValue());
      }
    }

    for (Object o: percentSink.collectedTuples) {
      @SuppressWarnings("unchecked")
      KeyValPair<String, Number> kv = (KeyValPair<String, Number>)o;
      if (kv.getKey().equals("a")) {
        Assert.assertEquals("change in a ", 50.0, kv.getValue());
      }
      if (kv.getKey().equals("b")) {
        Assert.assertEquals("change in b ", -80.0, kv.getValue());
      }
      if (kv.getKey().equals("c")) {
        Assert.assertEquals("change in c ", -96.0, kv.getValue());
      }
    }
  }
}
