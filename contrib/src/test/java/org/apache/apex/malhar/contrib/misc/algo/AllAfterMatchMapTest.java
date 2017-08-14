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
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 * @deprecated
 * Functional tests for {@link AllAfterMatchMapTest}
 * <p>
 */
@Deprecated
public class AllAfterMatchMapTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new AllAfterMatchMap<String, Integer>());
    testNodeProcessingSchema(new AllAfterMatchMap<String, Double>());
    testNodeProcessingSchema(new AllAfterMatchMap<String, Float>());
    testNodeProcessingSchema(new AllAfterMatchMap<String, Short>());
    testNodeProcessingSchema(new AllAfterMatchMap<String, Long>());
  }

  @SuppressWarnings({ "unchecked", "rawtypes", "unchecked" })
  public void testNodeProcessingSchema(AllAfterMatchMap oper)
  {
    CollectorTestSink allSink = new CollectorTestSink();
    oper.allafter.setSink(allSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 2);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 3);
    oper.data.process(input);

    input.clear();
    input.put("b", 6);
    oper.data.process(input);

    input.clear();
    input.put("c", 9);
    oper.data.process(input);

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3,
        allSink.collectedTuples.size());
    for (Object o : allSink.collectedTuples) {
      for (Map.Entry<String, Number> e : ((HashMap<String, Number>)o).entrySet()) {
        if (e.getKey().equals("a")) {
          Assert.assertEquals("emitted value for 'a' was ", new Double(3), new Double(e.getValue().doubleValue()));
        } else if (e.getKey().equals("b")) {
          Assert.assertEquals("emitted tuple for 'b' was ", new Double(6), new Double(e.getValue().doubleValue()));
        } else if (e.getKey().equals("c")) {
          Assert.assertEquals("emitted tuple for 'c' was ", new Double(9), new Double(e.getValue().doubleValue()));
        }
      }
    }
  }
}
