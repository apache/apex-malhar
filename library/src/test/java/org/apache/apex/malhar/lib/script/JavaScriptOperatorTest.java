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
package org.apache.apex.malhar.lib.script;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;

/**
 * Functional tests for {@link org.apache.apex.malhar.lib.script.JavaScriptOperator}.
 */
public class JavaScriptOperatorTest
{
  @Test
  public void testJavaOperator()
  {
    // Create bash operator instance (calculate suqare).
    JavaScriptOperator oper = new JavaScriptOperator();

    oper.addSetupScript("function square() { return val*val;}");
    oper.setInvoke("square");
    oper.setPassThru(true);
    oper.setup(null);
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    TestUtils.setSink(oper.result, sink);

    // Add input sample data.
    HashMap<String, Object> tuple = new HashMap<String, Object>();
    tuple.put("val", 2);

    // Process operator.
    oper.beginWindow(0);
    oper.inBindings.process(tuple);
    oper.endWindow();

    // Validate value.
    Assert.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());
    for (Object o : sink.collectedTuples) { // count is 12
      Assert.assertEquals("4.0 is expected", (Double)o, 4.0, 0);
    }
  }
}
