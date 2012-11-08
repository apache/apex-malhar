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
 * Functional tests for {@link com.malhartech.lib.math.Quotient}<p>
 *
 */
public class QuotientTest
{
  private static Logger LOG = LoggerFactory.getLogger(Quotient.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Quotient<String, Integer>());
    testNodeProcessingSchema(new Quotient<String, Double>());
  }

  public void testNodeProcessingSchema(Quotient oper) throws Exception
  {
    TestCountAndLastTupleSink quotientSink = new TestCountAndLastTupleSink();

    oper.quotient.setSink(quotientSink);
    oper.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));
    oper.setMult_by(2);

    oper.beginWindow(0); //
    HashMap<String, Number> input = new HashMap<String, Number>();
    int numtuples = 100;
    for (int i = 0; i < numtuples; i++) {
      input.clear();
      input.put("a", 2);
      input.put("b", 20);
      input.put("c", 1000);
      oper.numerator.process(input);
      input.clear();
      input.put("a", 2);
      input.put("b", 40);
      input.put("c", 500);
      oper.denominator.process(input);
    }

    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 1, quotientSink.count);
    HashMap<String, Number> output = (HashMap<String, Number>) quotientSink.tuple;
    for (Map.Entry<String, Number> e: output.entrySet()) {
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Double(2), e.getValue());
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", new Double(1), e.getValue());
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", new Double(4), e.getValue());
      }
      else {
        LOG.debug(String.format("key was %s", e.getKey()));
      }
    }
  }
}
