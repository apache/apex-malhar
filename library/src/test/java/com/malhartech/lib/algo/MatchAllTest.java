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
 * Functional tests for {@link com.malhartech.lib.algo.MatchAll}<p>
 *
 */
public class MatchAllTest
{
  private static Logger log = LoggerFactory.getLogger(MatchAllTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MatchAll<String, Integer>());
    testNodeProcessingSchema(new MatchAll<String, Double>());
    testNodeProcessingSchema(new MatchAll<String, Float>());
    testNodeProcessingSchema(new MatchAll<String, Short>());
    testNodeProcessingSchema(new MatchAll<String, Long>());
  }

  public void testNodeProcessingSchema(MatchAll oper)
  {
    TestCountAndLastTupleSink matchSink = new TestCountAndLastTupleSink();
    oper.all.setSink(matchSink);
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null));
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow();
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 3);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 3);
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, matchSink.count);
    Boolean result = (Boolean) matchSink.tuple;
    Assert.assertEquals("result was false", true, result.booleanValue());
    matchSink.clear();

    oper.beginWindow();
    input.put("a", 2);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 3);
    oper.data.process(input);
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 1, matchSink.count);
    result = (Boolean) matchSink.tuple;
    Assert.assertEquals("result was false", false, result.booleanValue());
    matchSink.clear();
  }
}