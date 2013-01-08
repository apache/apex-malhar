/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestCountAndLastTupleSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.Except}<p>
 *
 */
public class ExceptTest
{
  private static Logger log = LoggerFactory.getLogger(ExceptTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Except<String, Integer>());
    testNodeProcessingSchema(new Except<String, Double>());
    testNodeProcessingSchema(new Except<String, Float>());
    testNodeProcessingSchema(new Except<String, Short>());
    testNodeProcessingSchema(new Except<String, Long>());
  }

  public void testNodeProcessingSchema(Except oper)
  {
    TestCountAndLastTupleSink exceptSink = new TestCountAndLastTupleSink();
    oper.except.setSink(exceptSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 2);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 3);
    oper.data.process(input);
    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 1, exceptSink.count);
    for (Map.Entry<String, Number> e: ((HashMap<String, Number>) exceptSink.tuple).entrySet()) {
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Double(2), e.getValue().doubleValue());
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", new Double(20), e.getValue().doubleValue());
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", new Double(1000), e.getValue().doubleValue());
      }
    }
  }
}