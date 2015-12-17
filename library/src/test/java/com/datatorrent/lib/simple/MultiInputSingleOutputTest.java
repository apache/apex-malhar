package com.datatorrent.lib.simple;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.lib.testbench.CollectorTestSink;

import org.junit.Rule;
import org.junit.Test;

import org.junit.Assert;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MultiInputSingleOutputTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception {
    List<Integer> inputSizes =
        new ArrayList<Integer>(
            Arrays.asList(MultiInputSingleOutput.DEFAULT_NUM_INPUTS, 10, 100));
    List<Integer> valueSizes =
        new ArrayList<Integer>(Arrays.asList(10, 1000));

    // Test against the default constructor
    testOperator(valueSizes.get(0));

    // Test against the test matrix for this operator
    for(Integer input : inputSizes) {
      for (Integer value : valueSizes) {
        testOperator(input, value);
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
    MultiInputSingleOutput<String, Integer> oper = new MultiInputSingleOutput<String, Integer>(-1) {
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
    MultiInputSingleOutput<String, Integer> oper = new MultiInputSingleOutput<String, Integer>(65536) {
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
    MultiInputSingleOutput<String, Integer> oper = new MultiInputSingleOutput<String, Integer>(0) {
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
    MultiInputSingleOutput<String, Integer> oper = new MultiInputSingleOutput<String, Integer>() {
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
   * @param numInputs number of inputs for the operator
   * @param numValues number of tuples to process for the test
   */
  public void testOperator(int numInputs, int numValues) throws InstantiationException {
    MultiInputSingleOutput<String, Integer> oper = new MultiInputSingleOutput<String, Integer>(numInputs) {
      @Override
      public Integer process(String inputTuple) {
        return Integer.parseInt(inputTuple);
      }
    };

    testOperator(oper, numValues);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testOperator(MultiInputSingleOutput oper, int numValues) {
    int numInputs = oper.inputs.size();
    int sumTotal = 0;

    CollectorTestSink testSink = new CollectorTestSink();
    oper.output.setSink(testSink);

    for(int i = 1; i <= numValues; ++i) {
      for(int j = 0; j < numInputs; ++j) {
        ((DefaultInputPort)oper.inputs.get(j)).process(Integer.toString(i));
      }
    }

    int numTuples = numInputs * numValues;

    Assert.assertEquals("number emitted tuples", numTuples, testSink.collectedTuples.size());

    for(int i = 0; i < numTuples; ++i) {
      sumTotal += (Integer)testSink.collectedTuples.get(i);
    }

    Assert.assertEquals("correct values", ((numValues * (numValues + 1)) / 2) * numInputs, sumTotal);

    testSink.clear();
  }
}
