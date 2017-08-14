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
 * Functional tests for {@link MostFrequentKeyValueMap}<p>
 *
 */
@Deprecated
public class MostFrequentKeyValueMapTest
{
  /**
   * Test node logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    MostFrequentKeyValueMap<String, Integer> oper = new MostFrequentKeyValueMap<String, Integer>();
    CollectorTestSink matchSink = new CollectorTestSink();
    oper.most.setSink(matchSink);

    oper.beginWindow(0);
    HashMap<String, Integer> amap = new HashMap<String, Integer>(1);
    HashMap<String, Integer> bmap = new HashMap<String, Integer>(1);
    HashMap<String, Integer> cmap = new HashMap<String, Integer>(1);
    int atot1 = 5;
    int btot1 = 3;
    int ctot1 = 6;
    amap.put("a", 1);
    bmap.put("b", 2);
    cmap.put("c", 4);
    for (int i = 0; i < atot1; i++) {
      oper.data.process(amap);
    }
    for (int i = 0; i < btot1; i++) {
      oper.data.process(bmap);
    }
    for (int i = 0; i < ctot1; i++) {
      oper.data.process(cmap);
    }

    atot1 = 4;
    btot1 = 3;
    ctot1 = 10;
    amap.put("a", 5);
    bmap.put("b", 4);
    cmap.put("c", 3);
    for (int i = 0; i < atot1; i++) {
      oper.data.process(amap);
    }
    for (int i = 0; i < btot1; i++) {
      oper.data.process(bmap);
    }
    for (int i = 0; i < ctot1; i++) {
      oper.data.process(cmap);
    }

    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 3, matchSink.collectedTuples.size());
    int vcount;
    for (Object o: matchSink.collectedTuples) {
      HashMap<String, HashMap<Integer, Integer>> omap = (HashMap<String, HashMap<Integer, Integer>>)o;
      for (Map.Entry<String, HashMap<Integer, Integer>> e: omap.entrySet()) {
        String key = e.getKey();
        if (key.equals("a")) {
          vcount = e.getValue().get(1);
          Assert.assertEquals("Key \"a\" has value ", 5, vcount);

        } else if (key.equals("b")) {
          vcount = e.getValue().get(2);
          Assert.assertEquals("Key \"a\" has value ", 3, vcount);
          vcount = e.getValue().get(4);
          Assert.assertEquals("Key \"a\" has value ", 3, vcount);

        } else if (key.equals("c")) {
          vcount = e.getValue().get(3);
          Assert.assertEquals("Key \"a\" has value ", 10, vcount);
        }
      }
    }
  }
}
