/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.testbench.TestCountAndLastTupleSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.LastMatchMap}<p>
 *
 */
public class LastMatchMapTest
{
  private static Logger log = LoggerFactory.getLogger(LastMatchMapTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new LastMatchMap<String, Integer>());
    testNodeProcessingSchema(new LastMatchMap<String, Double>());
    testNodeProcessingSchema(new LastMatchMap<String, Float>());
    testNodeProcessingSchema(new LastMatchMap<String, Short>());
    testNodeProcessingSchema(new LastMatchMap<String, Long>());
  }

  public void testNodeProcessingSchema(LastMatchMap oper)
  {
    TestCountAndLastTupleSink matchSink = new TestCountAndLastTupleSink();
    oper.last.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 4);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.put("a", 3);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 2);
    oper.data.process(input);
    input.clear();
    input.put("a", 4);
    input.put("b", 21);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 3);
    input.put("b", 52);
    input.put("c", 5);
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, matchSink.count);
    HashMap<String, Number> tuple = (HashMap<String, Number>)matchSink.tuple;
    Number aval = tuple.get("a");
    Number bval = tuple.get("b");
    Assert.assertEquals("Value of a was ", 3, aval.intValue());
    Assert.assertEquals("Value of a was ", 52, bval.intValue());
    matchSink.clear();

    oper.beginWindow(0);
    input.clear();
    input.put("a", 2);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 5);
    oper.data.process(input);
    oper.endWindow();
    // There should be no emit as all tuples do not match
    Assert.assertEquals("number emitted tuples", 0, matchSink.count);
    matchSink.clear();
  }
}
