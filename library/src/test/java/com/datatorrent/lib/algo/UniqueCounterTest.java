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
package com.datatorrent.lib.algo;

import java.util.HashMap;

import org.junit.Assert;

import org.junit.Test;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.UniqueCounter}<p>
 *
 */
public class UniqueCounterTest
{
  /**
   * Test node logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    UniqueCounter<String> oper = new UniqueCounter<String>();
    CountAndLastTupleTestSink sink = new CountAndLastTupleTestSink<HashMap<String, Integer>>();
    oper.count.setSink(sink);

    String atuple = "a";
    String btuple = "b";
    String ctuple = "c";
    String dtuple = "d";
    String etuple = "e";

    int numTuples = 10000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(atuple);
      if (i % 2 == 0) {
        oper.data.process(btuple);
      }
      if (i % 3 == 0) {
        oper.data.process(ctuple);
      }
      if (i % 5 == 0) {
        oper.data.process(dtuple);
      }
      if (i % 10 == 0) {
        oper.data.process(etuple);
      }
    }
    oper.endWindow();
    HashMap<String, Integer> tuple = (HashMap<String, Integer>)sink.tuple;
    int acount = tuple.get("a");
    int bcount = tuple.get("b");
    int ccount = tuple.get("c");
    int dcount = tuple.get("d");
    int ecount = tuple.get("e");
    Assert.assertEquals("number emitted tuples", 1, sink.count);
    Assert.assertEquals("number emitted tuples", numTuples, acount);
    Assert.assertEquals("number emitted tuples", numTuples / 2, bcount);
    Assert.assertEquals("number emitted tuples", numTuples / 3 + 1, ccount);
    Assert.assertEquals("number emitted tuples", numTuples / 5, dcount);
    Assert.assertEquals("number emitted tuples", numTuples / 10, ecount);
  }
}
