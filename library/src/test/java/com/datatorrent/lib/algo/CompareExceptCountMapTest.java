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
 * Functional tests for {@link com.datatorrent.lib.algo.CompareExceptCountMap} <p>
 *
 */
public class CompareExceptCountMapTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new CompareExceptCountMap<String, Integer>());
    testNodeProcessingSchema(new CompareExceptCountMap<String, Double>());
    testNodeProcessingSchema(new CompareExceptCountMap<String, Float>());
    testNodeProcessingSchema(new CompareExceptCountMap<String, Short>());
    testNodeProcessingSchema(new CompareExceptCountMap<String, Long>());
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testNodeProcessingSchema(CompareExceptCountMap oper)
  {
    CountAndLastTupleTestSink countSink = new CountAndLastTupleTestSink();
    CountAndLastTupleTestSink exceptSink = new CountAndLastTupleTestSink();

    oper.count.setSink(countSink);
    oper.except.setSink(exceptSink);

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
    input.put("a", 5);
    oper.data.process(input);
    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 1, exceptSink.count);
    Assert.assertEquals("number emitted tuples", 1, countSink.count);
    Assert.assertEquals("number emitted tuples", "2", exceptSink.tuple.toString());
    Assert.assertEquals("number emitted tuples", "1", countSink.tuple.toString());
  }
}
