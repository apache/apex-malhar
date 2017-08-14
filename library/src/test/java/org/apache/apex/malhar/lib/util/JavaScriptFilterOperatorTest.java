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
package org.apache.apex.malhar.lib.util;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CountAndLastTupleTestSink;

/**
 *
 * functional test for {@link org.apache.apex.malhar.lib.util.JavaScriptFilterOperator}.
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
