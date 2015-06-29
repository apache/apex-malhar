/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.util;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
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
