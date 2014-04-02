/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
 * limitations under the License.
 */
package com.datatorrent.lib.logs;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.lib.testbench.CollectorTestSink;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional tests for {@link com.datatorrent.lib.logs.ApacheLogParseMapOutputOperator}.
 */
public class ApacheLogParseMapOutputOperatorTest
{
  private static Logger log = LoggerFactory.getLogger(ApacheLogParseMapOutputOperatorTest.class);

  /**
   * Test oper logic emits correct results
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testNodeProcessing()
  {

    ApacheLogParseMapOutputOperator oper = new ApacheLogParseMapOutputOperator();
    CollectorTestSink sink = new CollectorTestSink();
    oper.output.setSink(sink);
    oper.setRegexGroups(new String[] {null, "ipAddr", null, "userId", "date", "url", "httpCode", "bytes", null, "agent"});
    String token = "127.0.0.1 - - [04/Apr/2013:17:17:21 -0700] \"GET /favicon.ico HTTP/1.1\" 404 498 \"-\" \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 Safari/537.31\"";
    oper.setup(null);
    oper.beginWindow(0);
    oper.data.process(token);
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());
    Map<String, Object> map = (Map<String, Object>)sink.collectedTuples.get(0);
    log.debug("map {}", map);
    Assert.assertEquals("Size of map is 7", 7, map.size());
    Assert.assertEquals("checking ip", "127.0.0.1", map.get("ipAddr"));
    Assert.assertEquals("checking userid", "-", map.get("userId"));
    Assert.assertEquals("checking date", "04/Apr/2013:17:17:21 -0700", map.get("date"));
    Assert.assertEquals("checking url", "/favicon.ico", map.get("url"));
    Assert.assertEquals("checking http code", "404", map.get("httpCode"));
    Assert.assertEquals("checking bytes", "498", map.get("bytes"));
    Assert.assertEquals("checking agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 Safari/537.31", map.get("agent"));
  }

  /**
   * Test oper logic emits correct results
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testUserDefinedPattern()
  {

    ApacheLogParseMapOutputOperator oper = new ApacheLogParseMapOutputOperator();
    CollectorTestSink sink = new CollectorTestSink();
    oper.output.setSink(sink);
    oper.setRegexGroups(new String[] {null, "ipAddr", null, "userId", "date", "url", "httpCode", "rest"});
    String token = "127.0.0.1 - - [04/Apr/2013:17:17:21 -0700] \"GET /favicon.ico HTTP/1.1\" 404 498 \"-\" \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 Safari/537.31\"";
    oper.setLogRegex("^([\\d\\.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"[A-Z]+ (.+?) HTTP/\\S+\" (\\d{3})(.*)");
    oper.setup(null);
    oper.beginWindow(0);
    oper.data.process(token);
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());
    Map<String, Object> map = (Map<String, Object>)sink.collectedTuples.get(0);
    log.debug("map {}", map);
    Assert.assertEquals("Size of map is 6", 6, map.size());
    Assert.assertEquals("checking ip", "127.0.0.1", map.get("ipAddr"));
    Assert.assertEquals("checking userid", "-", map.get("userId"));
    Assert.assertEquals("checking date", "04/Apr/2013:17:17:21 -0700", map.get("date"));
    Assert.assertEquals("checking url", "/favicon.ico", map.get("url"));
    Assert.assertEquals("checking http code", "404", map.get("httpCode"));
    Assert.assertEquals("checking bytes", "498 \"-\" \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 Safari/537.31\"", map.get("rest"));
  }

}
