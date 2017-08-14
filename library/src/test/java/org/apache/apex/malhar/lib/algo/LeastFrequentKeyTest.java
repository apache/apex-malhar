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

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CountAndLastTupleTestSink;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.algo.LeastFrequentValue}<p>
 *
 */
public class LeastFrequentKeyTest
{
  /**
   * Test node logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    LeastFrequentValue<String> oper = new LeastFrequentValue<String>();
    CountAndLastTupleTestSink matchSink = new CountAndLastTupleTestSink();
    CountAndLastTupleTestSink listSink = new CountAndLastTupleTestSink();
    oper.least.setSink(matchSink);
    oper.list.setSink(listSink);

    oper.beginWindow(0);
    int atot = 5;
    int btot = 3;
    int ctot = 6;
    for (int i = 0; i < atot; i++) {
      oper.data.process("a");
    }
    for (int i = 0; i < btot; i++) {
      oper.data.process("b");
    }
    for (int i = 0; i < ctot; i++) {
      oper.data.process("c");
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 1, matchSink.count);
    HashMap<String, Integer> tuple = (HashMap<String, Integer>)matchSink.tuple;
    Integer val = tuple.get("b");
    Assert.assertEquals("Count of b was ", btot, val.intValue());
    Assert.assertEquals("number emitted tuples", 1, listSink.count);
    ArrayList<HashMap<String, Integer>> list = (ArrayList<HashMap<String, Integer>>)listSink.tuple;
    val = list.get(0).get("b");
    Assert.assertEquals("Count of b was ", btot, val.intValue());

    matchSink.clear();
    listSink.clear();
    oper.beginWindow(0);
    atot = 5;
    btot = 10;
    ctot = 5;
    for (int i = 0; i < atot; i++) {
      oper.data.process("a");
    }
    for (int i = 0; i < btot; i++) {
      oper.data.process("b");
    }
    for (int i = 0; i < ctot; i++) {
      oper.data.process("c");
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 1, matchSink.count);
    Assert.assertEquals("number emitted tuples", 1, listSink.count);
    list = (ArrayList<HashMap<String,Integer>>)listSink.tuple;
    int acount = 0;
    int ccount = 0;
    for (HashMap<String, Integer> h : list) {
      val = h.get("a");
      if (val == null) {
        ccount = h.get("c");
      } else {
        acount = val;
      }
    }
    Assert.assertEquals("Count of a was ", atot, acount);
    Assert.assertEquals("Count of c was ", ctot, ccount);
    HashMap<String, Integer> mtuple = (HashMap<String, Integer>)matchSink.tuple;
    val = mtuple.get("a");
    if (val == null) {
      val = mtuple.get("c");
    }
    Assert.assertEquals("Count of least frequent key was ", ctot, val.intValue());
  }
}
