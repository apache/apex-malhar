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
package org.apache.apex.malhar.lib.algo;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.HashTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.algo.Distinct}
 *
 */
public class DistinctTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    Distinct<String> oper = new Distinct<String>();

    HashTestSink<String> sortSink = new HashTestSink<String>();
    TestUtils.setSink(oper.distinct, sortSink);

    oper.beginWindow(0);
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("b");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("b");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("c");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("c");
    oper.data.process("d");
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 4, sortSink.size());
    Assert.assertEquals("number of \"a\"", 1, sortSink.getCount("a"));
    Assert.assertEquals("number of \"b\"", 1, sortSink.getCount("b"));
    Assert.assertEquals("number of \"c\"", 1, sortSink.getCount("c"));
    Assert.assertEquals("number of \"d\"", 1, sortSink.getCount("d"));
  }
}
