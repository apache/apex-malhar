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

import java.util.HashMap;

import org.junit.Assert;

import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CountAndLastTupleTestSink;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.algo.UniqueCounter}<p>
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

    int numTuples = 10000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(atuple);
      if (i % 2 == 0) {
        oper.data.process(btuple);
      }
      if (i % 5 == 0) {
        oper.data.process(ctuple);
      }
    }
    oper.endWindow();

    oper.beginWindow(1);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(atuple);
    }
    oper.endWindow();

    HashMap<String, Integer> tuple = (HashMap<String, Integer>)sink.tuple;
    int acount = tuple.get("a");
    Assert.assertEquals("number emitted tuples", numTuples, acount);

    oper.beginWindow(2);
    for (int i = 0; i < numTuples; i++) {
      if (i % 2 == 0) {
        oper.data.process(btuple);
      }
      oper.data.process(btuple);
      if (i % 10 == 0) {
        oper.data.process(dtuple);
      }
    }
    oper.endWindow();

    tuple = (HashMap<String, Integer>)sink.tuple;
    int bcount = tuple.get("b");
    int dcount = tuple.get("d");
    Assert.assertEquals("number emitted tuples", 3, sink.count);
    Assert.assertEquals("number emitted tuples", numTuples + (numTuples / 2), bcount);
    Assert.assertEquals("number emitted tuples", numTuples / 10, dcount);
  }
}
