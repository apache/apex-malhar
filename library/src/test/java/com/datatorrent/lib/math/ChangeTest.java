/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.math.Change;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.Change}. <p>
 *
 */
public class ChangeTest
{
  private static Logger log = LoggerFactory.getLogger(ChangeTest.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Change<Integer>());
    testNodeProcessingSchema(new Change<Double>());
    testNodeProcessingSchema(new Change<Float>());
    testNodeProcessingSchema(new Change<Short>());
    testNodeProcessingSchema(new Change<Long>());
  }

  /**
   *
   * @param oper
   */
  public <V extends Number> void testNodeProcessingSchema(Change<V> oper)
  {
    TestSink changeSink = new TestSink();
    TestSink percentSink = new TestSink();

    oper.change.setSink(changeSink);
    oper.percent.setSink(percentSink);

    oper.beginWindow(0);
    oper.base.process(oper.getValue(10));
    oper.data.process(oper.getValue(5));
    oper.data.process(oper.getValue(15));
    oper.data.process(oper.getValue(20));
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3, changeSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 3, percentSink.collectedTuples.size());

    log.debug("\nLogging tuples");
    for (Object o: changeSink.collectedTuples) {
      log.debug(String.format("change %s", o));
    }
    for (Object o: percentSink.collectedTuples) {
      log.debug(String.format("percent change %s", o));
    }
  }
}