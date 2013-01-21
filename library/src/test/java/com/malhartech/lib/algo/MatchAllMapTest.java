/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestCountAndLastTupleSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.MatchAllMap}<p>
 *
 */
public class MatchAllMapTest
{
  private static Logger log = LoggerFactory.getLogger(MatchAllMapTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MatchAllMap<String, Integer>());
    testNodeProcessingSchema(new MatchAllMap<String, Double>());
    testNodeProcessingSchema(new MatchAllMap<String, Float>());
    testNodeProcessingSchema(new MatchAllMap<String, Short>());
    testNodeProcessingSchema(new MatchAllMap<String, Long>());
  }

  public void testNodeProcessingSchema(MatchAllMap oper)
  {
    TestCountAndLastTupleSink matchSink = new TestCountAndLastTupleSink();
    oper.all.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
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

    oper.beginWindow(0);
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
