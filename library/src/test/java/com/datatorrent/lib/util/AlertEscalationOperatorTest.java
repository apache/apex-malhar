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
    Thread.sleep(1000);
    String s = "hello";
    oper.in.process(s); // should get an alert here
    s = "world";
    oper.in.process(s); // should not get an alert because of alert interval
    s = "hello";
    oper.in.process(s); // should not get an alert because of alert interval
    Thread.sleep(1500);
    s = "world";
    oper.in.process(s); // should get another alert here.
    Thread.sleep(1000);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 2, matchSink.count);
  }

}
