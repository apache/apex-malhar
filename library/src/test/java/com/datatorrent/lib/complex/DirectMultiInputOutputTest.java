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
package com.datatorrent.lib.complex;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.testbench.CollectorTestSink;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DirectMultiInputOutputTest {
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception {
    List<Integer> inputOutputSizes =
        new ArrayList<Integer>(Arrays.asList(DirectMultiInputOutput.DEFAULT_NUM_INPUTS_OUTPUTS, 10, 100));
    List<Integer> valueSizes =
        new ArrayList<Integer>(Arrays.asList(10, 1000));

    // Test against the default constructor
    testOperator(valueSizes.get(0));

    // Test against the test matrix for this operator
    for(Integer inputOutput : inputOutputSizes) {
      for(Integer value : valueSizes) {
        testOperator(inputOutput, value);
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
    DirectMultiInputOutput<String, Integer> oper = new DirectMultiInputOutput<String, Integer>(-1) {
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
    DirectMultiInputOutput<String, Integer> oper = new DirectMultiInputOutput<String, Integer>(65536) {
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
    DirectMultiInputOutput<String, Integer> oper = new DirectMultiInputOutput<String, Integer>(0) {
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
    DirectMultiInputOutput<String, Integer> oper = new DirectMultiInputOutput<String, Integer>() {
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
   * @param numInputsOutputs number of inputs and outputs for the operator
   * @param numValues number of tuples to process for the test
   */
  public void testOperator(int numInputsOutputs, int numValues) throws InstantiationException {
    DirectMultiInputOutput<String, Integer> oper = new DirectMultiInputOutput<String, Integer>(numInputsOutputs) {
      @Override
      public Integer process(String inputTuple) {
        return Integer.parseInt(inputTuple);
      }
    };

    testOperator(oper, numValues);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testOperator(DirectMultiInputOutput oper, int numValues) {
    int numInputsOutputs = oper.inputs.size();

    int[] sumTotals = new int[numInputsOutputs];
    CollectorTestSink[] testSinks = new CollectorTestSink[numInputsOutputs];

    for(int i = 0; i < numInputsOutputs; ++i) {
      testSinks[i] = new CollectorTestSink();
      ((DefaultOutputPort)oper.outputs.get(i)).setSink(testSinks[i]);
    }

    for(int i = 1; i <= numValues; ++i) {
      for(int j = 0; j < numInputsOutputs; ++j) {
        ((DefaultInputPort)oper.inputs.get(j)).process(Integer.toString(i));
      }
    }

    for(int i = 0; i < numInputsOutputs; ++i) {
      Assert.assertEquals("number emitted tuples", numValues, testSinks[i].collectedTuples.size());

      sumTotals[i] = 0;
      for(int j = 0; j < numValues; ++j) {
        sumTotals[i] += (Integer)testSinks[i].collectedTuples.get(j);
      }

      Assert.assertEquals("correct values", (1 + numValues) * (numValues / 2), sumTotals[i]);
    }

    for(int i = 0; i < numInputsOutputs; ++i) {
      testSinks[i].clear();
    }
  }
}
