/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.lib.testbench.CountAndLastTupleTestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.CompareExceptStringMap}<p>
 *
 */
public class CompareExceptStringMapTest
{
  private static Logger log = LoggerFactory.getLogger(CompareExceptStringMapTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessingSchema()
  {
    CompareExceptStringMap<String> oper = new CompareExceptStringMap<String>();
    CountAndLastTupleTestSink compareSink = new CountAndLastTupleTestSink();
    CountAndLastTupleTestSink exceptSink = new CountAndLastTupleTestSink();
    oper.compare.setSink(compareSink);
    oper.except.setSink(exceptSink);

    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, String> input = new HashMap<String, String>();
    input.put("a", "2");
    input.put("b", "20");
    input.put("c", "1000");
    oper.data.process(input);
    input.clear();
    input.put("a", "3");
    input.put("b", "32");
    input.put("c", "23");
    oper.data.process(input);
    oper.endWindow();

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

    Assert.assertEquals("number emitted tuples", 1, compareSink.count);
    for (Map.Entry<String, String> e: ((HashMap<String, String>) compareSink.tuple).entrySet()) {
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", "3", e.getValue());
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ",  "32", e.getValue());
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", "23", e.getValue());
      }
    }
  }
}