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
 * Functional tests for {@link com.malhartech.lib.math.CompareString}<p>
 *
 */
public class CompareStringTest
{
  private static Logger log = LoggerFactory.getLogger(CompareStringTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessingSchema()
  {
    CompareString<String> oper = new CompareString<String>();
    TestCountAndLastTupleSink exceptSink = new TestCountAndLastTupleSink();
    oper.compare.setSink(exceptSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeNEQ();

    oper.beginWindow(0);
    HashMap<String, String> input = new HashMap<String, String>();
    input.put("a", "2");
    input.put("b", "20");
    input.put("c", "1000");
    oper.data.process(input);
    input.clear();
    input.put("a", "3");
    oper.data.process(input);
    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 1, exceptSink.count);
    for (Map.Entry<String, String> e: ((HashMap<String, String>) exceptSink.tuple).entrySet()) {
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", "2", e.getValue());
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ",  "20", e.getValue());
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", "1000", e.getValue());
      }
    }
  }
}