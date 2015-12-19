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

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.testbench.CollectorTestSink;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SingleInputMultiOutputTest {
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception {
    List<Integer> outputSizes =
        new ArrayList<Integer>(
            Arrays.asList(SingleInputMultiOutput.DEFAULT_NUM_OUTPUTS, 10, 100));
    List<Integer> valueSizes =
        new ArrayList<Integer>(Arrays.asList(10, 1000));

    // Test against the default constructor
    testOperator(valueSizes.get(0));

    // Test against the test matrix for this operator
    for(Integer output : outputSizes) {
      for (Integer value : valueSizes) {
        testOperator(output, value);
      }
    }
  }

  /**
   * Test invalid operator instantiation with negative value.
   *
   * @throws InstantiationException
   */
  @Test(expected=InstantiationException.class)
  public void testNegativeOperatorInstantiation() throws InstantiationException {
    SingleInputMultiOutput<String, Integer> oper = new SingleInputMultiOutput<String, Integer>(-1) {
      @Override
      public Integer process(String inputTuple) {
        return null;
      }
    };
  }

  /**
   * Test invalid operator instantiation with positive value.
   *
   * @throws InstantiationException
   */
  @Test(expected=InstantiationException.class)
  public void testPositiveOperatorInstantiation() throws InstantiationException {
    SingleInputMultiOutput<String, Integer> oper = new SingleInputMultiOutput<String, Integer>(65536) {
      @Override
      public Integer process(String inputTuple) {
        return null;
      }
    };
  }

  /**
   * Test invalid operator instantiation with zero value.
   *
   * @throws InstantiationException
   */
  @Test(expected=InstantiationException.class)
  public void testZeroOperatorInstantiation() throws InstantiationException {
    SingleInputMultiOutput<String, Integer> oper = new SingleInputMultiOutput<String, Integer>(0) {
      @Override
      public Integer process(String inputTuple) {
        return null;
      }
    };
  }

  /**
   * Test the default constructor.
   *
   * @param numValues number of tuples to process for the test
   */
  public void testOperator(int numValues) throws InstantiationException {
    SingleInputMultiOutput<String, Integer> oper = new SingleInputMultiOutput<String, Integer>() {
      @Override
      public Integer process(String inputTuple) {
        return Integer.parseInt(inputTuple);
      }
    };

    testOperator(oper, numValues);
  }

  /**
   * Test the various ways the operator could be constructed.
   *
   * @param numOutputs number of outputs for the operator
   * @param numValues number of tuples to process for the test
   */
  public void testOperator(final int numOutputs, int numValues) throws InstantiationException {
    SingleInputMultiOutput<String, Integer> oper = new SingleInputMultiOutput<String, Integer>(numOutputs) {
      @Override
      public Integer process(String inputTuple) {
        return Integer.parseInt(inputTuple);
      }
    };

    testOperator(oper, numValues);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testOperator(SingleInputMultiOutput oper, int numValues) {
    int numOutputs = oper.outputs.size();

    int[] sumTotals = new int[numOutputs];
    CollectorTestSink[] testSinks = new CollectorTestSink[numOutputs];

    for(int i = 0; i < numOutputs; ++i) {
      testSinks[i] = new CollectorTestSink();
      ((DefaultOutputPort)oper.outputs.get(i)).setSink(testSinks[i]);
    }

    for(int i = 1; i <= numValues; ++i) {
      oper.input.process(Integer.toString(i));
    }

    for(int i = 0; i < numOutputs; ++i) {
      Assert.assertEquals("number emitted tuples", numValues, testSinks[i].collectedTuples.size());

      sumTotals[i] = 0;
      for(int j = 0; j < numValues; ++j) {
        sumTotals[i] += (Integer)testSinks[i].collectedTuples.get(j);
      }

      Assert.assertEquals("correct values", (1 + numValues) * (numValues / 2), sumTotals[i]);
    }

    for(int i = 0; i < numOutputs; ++i) {
      testSinks[i].clear();
    }
  }
}
