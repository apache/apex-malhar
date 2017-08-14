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

import org.junit.Assert;

import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.algo.InsertSort}<p>
 */
public class InsertSortTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new InsertSort<Integer>(), "Integer");
    testNodeProcessingSchema(new InsertSort<Double>(), "Double");
    testNodeProcessingSchema(new InsertSort<Float>(), "Float");
    testNodeProcessingSchema(new InsertSort<String>(), "String");
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testNodeProcessingSchema(InsertSort oper, String debug)
  {
    CollectorTestSink sortSink = new CollectorTestSink();
    oper.sort.setSink(sortSink);

    ArrayList input = new ArrayList();

    oper.beginWindow(0);

    input.add(2);
    oper.datalist.process(input);
    oper.data.process(20);

    input.clear();
    input.add(1000);
    input.add(5);
    input.add(20);
    input.add(33);
    input.add(33);
    input.add(34);
    oper.datalist.process(input);

    input.clear();
    input.add(34);
    input.add(1001);
    input.add(6);
    input.add(1);
    input.add(33);
    input.add(9);
    oper.datalist.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    input = (ArrayList)sortSink.collectedTuples.get(0);
  }
}
