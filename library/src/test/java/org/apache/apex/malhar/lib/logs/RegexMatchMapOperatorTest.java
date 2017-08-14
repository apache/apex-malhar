/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.logs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 * Functional tests for {@link org.apache.apex.malhar.lib.logs.RegexMatchMapOperator}.
 */
public class RegexMatchMapOperatorTest
{
  private static final Logger log = LoggerFactory.getLogger(RegexMatchMapOperatorTest.class);

  /**
   * Since the regex does not have a default value, ensure the operator raises a RuntimeException if it is not set.
   */
  @Test(expected = RuntimeException.class)
  public void testRaisesIfNoRegex()
  {
    String string = "foobar";

    // set up the operator
    RegexMatchMapOperator oper = new RegexMatchMapOperator();

    // stub output port
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    oper.output.setSink(sink);

    // process the string
    oper.beginWindow(0);
    oper.data.process(string); // this should raise since regex has not been set
    oper.endWindow();
  }

  /**
   * Test operator does not emit anything if the regex matches nothing
   */
  @Test
  public void testNoMatches()
  {
    String string = "abcd";
    String regex = "^(?<myfield>\\d+)"; // will not match anything in the string

    // set up the operator with the regex
    RegexMatchMapOperator oper = new RegexMatchMapOperator();
    oper.setRegex(regex);

    // stub output port
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    oper.output.setSink(sink);

    // process the string
    oper.beginWindow(0);
    oper.data.process(string);
    oper.endWindow();

    // debugging output
    log.debug(String.format("Line is  : %s", string));
    log.debug(String.format("Regex is : %s", regex));

    // ensure nothing was emitted
    Assert.assertEquals("number emitted tuples", 0, sink.collectedTuples.size());
  }

  /**
   * Test operator logic parses strings correctly for given regexes
   */
  @Test
  public void testMatching()
  {
    ArrayList<HashMap<String, String>> test_cases = new ArrayList<HashMap<String, String>>();

    // class comment example case
    HashMap<String, String> test_case = new HashMap<String, String>();
    test_case.put("string", "12345 \"foo bar\" baz;goober");
    test_case.put("regex", "^(?<id>\\d+) \"(?<username>[^\"]+)\" (?<action>[^;]+);(?<cookie>.+)");
    test_case.put("fields", "id,username,action,cookie");
    test_cases.add(test_case);

    // apache log case
    test_case = new HashMap<String, String>();
    test_case.put("string",
        "127.0.0.1 - - [04/Apr/2013:17:17:21 -0700] \"GET /favicon.ico HTTP/1.1\" 404 498 \"http://www.google.com/\" " +
        "\"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 Safari/537" +
        ".31\"");

    test_case.put("regex",
        "^(?<ip>[\\d\\.]+) (\\S+) (\\S+) \\[(?<date>[\\w:/]+\\s[+\\-]\\d{4})\\] \"[A-Z]+ (?<url>.+?) HTTP/\\S+\" " +
        "(?<status>\\d{3}) (?<bytes>\\d+) \"(?<referer>[^\"]+)\" \"(?<agent>[^\"]+)\"");

    test_case.put("fields", "ip,date,url,status,bytes,referer,agent");
    test_cases.add(test_case);

    // iterate through test cases
    for (HashMap<String, String> curr_case : test_cases) {
      String string = curr_case.get("string");
      String regex = curr_case.get("regex");
      String[] fields = curr_case.get("fields").split(",");

      // set up the operator with the regex
      RegexMatchMapOperator oper = new RegexMatchMapOperator();
      oper.setRegex(regex);

      // stub output port
      CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
      oper.output.setSink(sink);

      // process the string
      oper.beginWindow(0);
      oper.data.process(string);
      oper.endWindow();

      // ensure a successful capture
      Assert.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());

      // fetch the Map that was output
      @SuppressWarnings("unchecked")
      Map<String, Object> output = (Map<String, Object>)sink.collectedTuples.get(0);

      // debugging output
      log.debug(String.format("Line is  : %s", string));
      log.debug(String.format("Regex is : %s", regex));
      log.debug(String.format("Map is   : %s", output.toString()));

      for (String field : fields) {
        // ensure keys are set
        Assert.assertTrue("Map is missing '" + field + "' key", output.containsKey(field));
      }
    }
  }
}
