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
 * Functional tests for {@link com.malhartech.lib.algo.LastMatch}<p>
 *
 */
public class LastMatchStringMapTest
{
  private static Logger log = LoggerFactory.getLogger(LastMatchStringMapTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    LastMatchStringMap<String> oper = new LastMatchStringMap<String>();
    TestCountAndLastTupleSink matchSink = new TestCountAndLastTupleSink();
    oper.last.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, String> input = new HashMap<String, String>();
    input.put("a", "4");
    input.put("b", "20");
    input.put("c", "1000");
    oper.data.process(input);
    input.put("a", "3");
    input.put("b", "20");
    input.put("c", "1000");
    oper.data.process(input);
    input.clear();
    input.put("a", "2");
    oper.data.process(input);
    input.clear();
    input.put("a", "4");
    input.put("b", "21");
    input.put("c", "1000");
    oper.data.process(input);
    input.clear();
    input.put("a", "3");
    input.put("b", "52");
    input.put("c", "5");
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, matchSink.count);
    HashMap<String, String> tuple = (HashMap<String, String>)matchSink.tuple;
    Assert.assertEquals("Value of a was ", "3", tuple.get("a"));
    Assert.assertEquals("Value of a was ", "52", tuple.get("b"));
    matchSink.clear();

    oper.beginWindow(0);
    input.clear();
    input.put("a", "2");
    input.put("b", "20");
    input.put("c", "1000");
    oper.data.process(input);
    input.clear();
    input.put("a", "5");
    oper.data.process(input);
    oper.endWindow();
    // There should be no emit as all tuples do not match
    Assert.assertEquals("number emitted tuples", 0, matchSink.count);
    matchSink.clear();
  }
}
