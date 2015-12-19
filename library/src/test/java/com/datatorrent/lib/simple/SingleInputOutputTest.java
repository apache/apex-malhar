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
package com.datatorrent.lib.simple;

import com.datatorrent.lib.testbench.CollectorTestSink;

import org.junit.Test;

import org.junit.Assert;

public class SingleInputOutputTest {
  int numValues = 100;
  /**
   * Test node logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception {
    SingleInputOutput<String, Integer> oper = new SingleInputOutput<String, Integer>() {
      @Override
      Integer process(String inputTuple) {
        return Integer.parseInt(inputTuple);
      }
    };
    CollectorTestSink testSink = new CollectorTestSink();
    oper.output.setSink(testSink);

    for(int i = 0; i < numValues; ++i) {
      oper.input.process(Integer.toString(i));
    }

    Assert.assertEquals("number emitted tuples", numValues, testSink.collectedTuples.size());

    for(int i = 0; i < numValues; ++i) {
      Assert.assertEquals("correct values", i, testSink.collectedTuples.get(i));
    }

    testSink.clear();
  }
}
