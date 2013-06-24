/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.logs;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.logs.ApacheLogParseOperator;
import com.datatorrent.lib.testbench.ArrayListTestSink;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import com.datatorrent.lib.testbench.CountTestSink;
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
 * Functional tests for {@link com.datatorrent.lib.logs.ApacheLogParseOperator}<p>
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
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
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
