/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.dag.TestCountAndLastTupleSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.LastMatch}<p>
 *
 */
public class LastMatchTest
{
  private static Logger log = LoggerFactory.getLogger(LastMatchTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new LastMatch<String, Integer>());
    testNodeProcessingSchema(new LastMatch<String, Double>());
    testNodeProcessingSchema(new LastMatch<String, Float>());
    testNodeProcessingSchema(new LastMatch<String, Short>());
    testNodeProcessingSchema(new LastMatch<String, Long>());
  }

  public void testNodeProcessingSchema(LastMatch oper)
  {
    TestCountAndLastTupleSink matchSink = new TestCountAndLastTupleSink();
    oper.last.setSink(matchSink);
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null));
    oper.setKey("a");
    oper.setValue(3);
    oper.setTypeEQ();

    oper.beginWindow();
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

    oper.beginWindow();
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