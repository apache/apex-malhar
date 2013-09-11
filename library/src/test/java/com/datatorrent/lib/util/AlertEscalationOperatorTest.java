/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * functional test for {@link com.datatorrent.lib.util.JavaScriptFilterOperator}.
 */
public class AlertEscalationOperatorTest
{
  @Test
  public void testEscalation() throws InterruptedException
  {
    AlertEscalationOperator oper = new AlertEscalationOperator();
    oper.setAlertInterval(1000);
    oper.setTimeout(2000);
    CountAndLastTupleTestSink<Object> matchSink = new CountAndLastTupleTestSink<Object>();
    oper.alert.setSink(matchSink);
    oper.setup(null);
    oper.beginWindow(0);
    String s = "hello";
    oper.in.process(s);
    Thread.sleep(1000);
    s = "world";
    oper.in.process(s); // should get an alert here now
    s = "hello";
    oper.in.process(s);
    Thread.sleep(1000);
    s = "world";
    oper.in.process(s); // shold get another alert here.
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 2, matchSink.count);
  }

}
