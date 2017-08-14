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
 * Functional tests for {@link DistinctMap}<p>
 *
 */
@Deprecated
public class DistinctMapTest
{
  /**
   * Test node logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    DistinctMap<String, Number> oper = new DistinctMap<String, Number>();

    CollectorTestSink sortSink = new CollectorTestSink();
    oper.distinct.setSink(sortSink);


    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();

    input.put("a", 2);
    oper.data.process(input);
    input.clear();
    input.put("a", 2);
    oper.data.process(input);

    input.clear();
    input.put("a", 1000);
    oper.data.process(input);

    input.clear();
    input.put("a", 5);
    oper.data.process(input);

    input.clear();
    input.put("a", 2);
    input.put("b", 33);
    oper.data.process(input);

    input.clear();
    input.put("a", 33);
    input.put("b", 34);
    oper.data.process(input);

    input.clear();
    input.put("b", 34);
    oper.data.process(input);

    input.clear();
    input.put("b", 6);
    input.put("a", 2);
    oper.data.process(input);
    input.clear();
    input.put("c", 9);
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 8, sortSink.collectedTuples.size());
    int aval = 0;
    int bval = 0;
    int cval = 0;
    for (Object o: sortSink.collectedTuples) {
      for (Map.Entry<String, Integer> e: ((HashMap<String, Integer>)o).entrySet()) {
        String key = e.getKey();
        if (key.equals("a")) {
          aval += e.getValue();
        } else if (key.equals("b")) {
          bval += e.getValue();
        } else if (key.equals("c")) {
          cval += e.getValue();
        }
      }
    }
    Assert.assertEquals("Total for key \"a\" ", 1040, aval);
    Assert.assertEquals("Total for key \"a\" ", 73, bval);
    Assert.assertEquals("Total for key \"a\" ", 9, cval);
  }
}
