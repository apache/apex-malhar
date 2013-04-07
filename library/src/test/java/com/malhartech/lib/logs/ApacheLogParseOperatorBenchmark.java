/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.engine.TestSink;
import com.malhartech.lib.testbench.ArrayListTestSink;
import com.malhartech.lib.testbench.CountAndLastTupleTestSink;
import com.malhartech.lib.testbench.CountTestSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.logs.ApacheLogParseOperator}<p>
 *
 */
public class ApacheLogParseOperatorBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ApacheLogParseOperatorBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    ApacheLogParseOperator oper = new ApacheLogParseOperator();
    CountTestSink ipSink = new CountTestSink();
    CountTestSink urlSink = new CountTestSink();
    CountTestSink scSink = new CountTestSink();
    CountTestSink bytesSink = new CountTestSink();
    CountTestSink refSink = new CountTestSink();
    CountTestSink agentSink = new CountTestSink();

    oper.outputIPAddress.setSink(ipSink);
    oper.outputUrl.setSink(urlSink);
    oper.outputStatusCode.setSink(scSink);
    oper.outputBytes.setSink(bytesSink);
    oper.outputReferer.setSink(refSink);
    oper.outputAgent.setSink(agentSink);

    String token = "127.0.0.1 - - [04/Apr/2013:17:17:21 -0700] \"GET /favicon.ico HTTP/1.1\" 404 498 \"-\" \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 Safari/537.31\"";
    oper.beginWindow(0);
    int total = 1000000;
    for (int i = 0; i < total; i++) {
      oper.data.process(token);
    }
    oper.endWindow(); //

    Assert.assertEquals("number emitted tuples", total, ipSink.getCount());
    Assert.assertEquals("number emitted tuples", total, urlSink.getCount());
    Assert.assertEquals("number emitted tuples", total, scSink.getCount());
    Assert.assertEquals("number emitted tuples", total, bytesSink.getCount());
    Assert.assertEquals("number emitted tuples", total, refSink.getCount());
    Assert.assertEquals("number emitted tuples", total, agentSink.getCount());
  }
}
