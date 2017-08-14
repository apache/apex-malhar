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

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.HashTestSink;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.logs.LineTokenizer}<p>
 *
 */
public class LineTokenizerTest
{
  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeProcessing()
  {

    LineTokenizer oper = new LineTokenizer();
    HashTestSink<Object> tokenSink = new HashTestSink<Object>();

    oper.setSplitBy(",");
    oper.tokens.setSink(tokenSink);
    oper.beginWindow(0); //

    String input1 = "a,b,c";
    String input2 = "a";
    String input3 = "";
    int numTuples = 1000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow(); //
    Assert.assertEquals("number emitted tuples", 3, tokenSink.map.size());
  }
}
