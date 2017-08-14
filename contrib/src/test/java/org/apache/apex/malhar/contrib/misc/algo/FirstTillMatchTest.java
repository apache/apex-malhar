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
package org.apache.apex.malhar.contrib.misc.algo;

import java.util.HashMap;

import org.junit.Assert;

import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 * @deprecated
 * Functional tests for {@link FirstTillMatch}<p>
 *
 */
@Deprecated
public class FirstTillMatchTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new FirstTillMatch<String, Integer>());
    testNodeProcessingSchema(new FirstTillMatch<String, Double>());
    testNodeProcessingSchema(new FirstTillMatch<String, Float>());
    testNodeProcessingSchema(new FirstTillMatch<String, Short>());
    testNodeProcessingSchema(new FirstTillMatch<String, Long>());
  }

  @SuppressWarnings( {"unchecked", "rawtypes"})
  public void testNodeProcessingSchema(FirstTillMatch oper)
  {
    CollectorTestSink matchSink = new CollectorTestSink();
    oper.first.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 4);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 2);
    oper.data.process(input);
    input.put("a", 3);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 4);
    input.put("b", 21);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 6);
    input.put("b", 20);
    input.put("c", 5);
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 2, matchSink.collectedTuples.size());
    int atotal = 0;
    for (Object o: matchSink.collectedTuples) {
      atotal += ((HashMap<String,Number>)o).get("a").intValue();
    }
    Assert.assertEquals("Value of a was ", 6, atotal);
    matchSink.clear();

    oper.beginWindow(0);
    input.clear();
    input.put("a", 2);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 5);
    oper.data.process(input);
    oper.endWindow();
    // There should be no emit as all tuples do not match
    Assert.assertEquals("number emitted tuples", 2, matchSink.collectedTuples.size());
    atotal = 0;
    for (Object o: matchSink.collectedTuples) {
      atotal += ((HashMap<String,Number>)o).get("a").intValue();
    }
    Assert.assertEquals("Value of a was ", 7, atotal);
  }
}
