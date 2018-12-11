/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * functional test for {@link com.datatorrent.lib.util.JavaScriptFilterOperator}.
 */
public class JavaScriptFilterOperatorTest
{
  @Test
  public void testFilter()
  {
    JavaScriptFilterOperator oper = new JavaScriptFilterOperator();
    oper.setSetupScript("function foo() { return a + b > 10 }");
    oper.setFunctionName("foo");
    CountAndLastTupleTestSink<Object> matchSink = new CountAndLastTupleTestSink<Object>();
    oper.out.setSink(matchSink);
    oper.setup(null);
    oper.beginWindow(0);
    Map<String, Object> m = new HashMap<String, Object>();
    m.put("a", 5);
    m.put("b", 4);
    oper.in.process(m);
    m = new HashMap<String, Object>();
    m.put("a", 7);
    m.put("b", 8);
    oper.in.process(m);
    m = new HashMap<String, Object>();
    m.put("a", 1);
    m.put("b", 3);
    oper.in.process(m);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, matchSink.count);
    Map<String, Object> tuple = (Map<String, Object>)matchSink.tuple;
    Integer val = (Integer)tuple.get("a");
    Assert.assertEquals("a should be 7", 7, val.intValue());
    val = (Integer)tuple.get("b");
    Assert.assertEquals("b should be 8", 8, val.intValue());
  }

}
