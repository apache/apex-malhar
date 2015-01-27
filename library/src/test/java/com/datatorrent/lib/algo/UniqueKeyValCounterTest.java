/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.algo;

import java.util.HashMap;

import org.junit.Assert;

import org.junit.Test;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.UniqueKeyValCounter}<p>
 *
 */
public class UniqueKeyValCounterTest
{
  /**
   * Test node logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    UniqueKeyValCounter<String,Integer> oper = new UniqueKeyValCounter<String,Integer>();
    CountAndLastTupleTestSink sink = new CountAndLastTupleTestSink<HashMap<HashMap<String,Integer>, Integer>>();
    oper.count.setSink(sink);

    HashMap<String,Integer> a1tuple = new HashMap<String,Integer>(1);
    HashMap<String,Integer> a2tuple = new HashMap<String,Integer>(1);
    HashMap<String,Integer> btuple = new HashMap<String,Integer>(1);
    HashMap<String,Integer> ctuple = new HashMap<String,Integer>(1);
    HashMap<String,Integer> dtuple = new HashMap<String,Integer>(1);
    HashMap<String,Integer> e1tuple = new HashMap<String,Integer>(1);
    HashMap<String,Integer> e2tuple = new HashMap<String,Integer>(1);

    a1tuple.put("a",1);
    a2tuple.put("a",2);
    btuple.put("b",4);
    ctuple.put("c",3);
    dtuple.put("d",1001);
    e1tuple.put("e",1091919);
    e2tuple.put("e",808);
    e2tuple.put("c",3);


    int numTuples = 10000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(a1tuple);
      if (i % 2 == 0) {
        oper.data.process(btuple);
        oper.data.process(e2tuple);
      }
      if (i % 3 == 0) {
        oper.data.process(ctuple);
      }
      if (i % 5 == 0) {
        oper.data.process(dtuple);
        oper.data.process(a2tuple);
      }
      if (i % 10 == 0) {
        oper.data.process(e1tuple);
      }
    }
    oper.endWindow();
    HashMap<HashMap<String,Integer>,Integer> tuple = (HashMap<HashMap<String,Integer>,Integer>) sink.tuple;
    int a1count = tuple.get(a1tuple);
    int a2count = tuple.get(a2tuple);
    int bcount = tuple.get(btuple);
    int ccount = tuple.get(ctuple);
    int dcount = tuple.get(dtuple);
    int e1count = tuple.get(e1tuple);
    e2tuple.clear();
    e2tuple.put("e",808);
    int e2count = tuple.get(e2tuple);

    Assert.assertEquals("number emitted tuples", 1, sink.count);
    Assert.assertEquals("number emitted tuples", numTuples, a1count);
    Assert.assertEquals("number emitted tuples", numTuples/5, a2count);
    Assert.assertEquals("number emitted tuples", numTuples/2, bcount);
    Assert.assertEquals("number emitted tuples", (numTuples/3 + 1) + numTuples/2, ccount);
    Assert.assertEquals("number emitted tuples", numTuples/5, dcount);
    Assert.assertEquals("number emitted tuples", numTuples/10, e1count);
    Assert.assertEquals("number emitted tuples", numTuples/2, e2count);
  }
}
