package com.datatorrent.lib.complex;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.testbench.CollectorTestSink;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AllWayMultiInputOutputTest {
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception {
    List<Integer> inputSizes =
        new ArrayList<Integer>(Arrays.asList(AllWayMultiInputOutput.DEFAULT_NUM_INPUTS, 10, 100));
    List<Integer> outputSizes =
        new ArrayList<Integer>(Arrays.asList(AllWayMultiInputOutput.DEFAULT_NUM_OUTPUTS, 10, 100));
    List<Integer> valueSizes =
        new ArrayList<Integer>(Arrays.asList(10, 1000));

    // Test against the default constructor
    testOperator(valueSizes.get(0));

    // Test against the test matrix for this operator
    for(Integer input : inputSizes) {
      for(Integer output : outputSizes) {
        for(Integer value : valueSizes) {
          testOperator(input, output, value);
        }
      }
    }
  }

  /**
   * Test invalid operator instantiation with negative value for input ports.
   *
   * @throws InstantiationException
   */
  @Test(expected=InstantiationException.class)
  public void testNegativeInputOperatorInstantiation() throws InstantiationException {
    AllWayMultiInputOutput<String, Integer> oper = new AllWayMultiInputOutput<String, Integer>(-1, 1) {
      @Override
      public Integer process(String inputTuple) {
        return null;
      }
    };
  }

  /**
   * Test invalid operator instantiation with positive value for input ports.
   *
   * @throws InstantiationException
   */
  @Test(expected=InstantiationException.class)
  public void testPositiveInputOperatorInstantiation() throws InstantiationException {
    AllWayMultiInputOutput<String, Integer> oper = new AllWayMultiInputOutput<String, Integer>(65536, 1) {
      @Override
      public Integer process(String inputTuple) {
        return null;
      }
    };
  }

  /**
   * Test invalid operator instantiation with zero value for input ports.
   *
   * @throws InstantiationException
   */
  @Test(expected=InstantiationException.class)
  public void testZeroInputOperatorInstantiation() throws InstantiationException {
    AllWayMultiInputOutput<String, Integer> oper = new AllWayMultiInputOutput<String, Integer>(0, 1) {
      @Override
      public Integer process(String inputTuple) {
        return null;
      }
    };
  }

  /**
   * Test invalid operator instantiation with negative value for output ports.
   *
   * @throws InstantiationException
   */
  @Test(expected=InstantiationException.class)
  public void testNegativeOutputOperatorInstantiation() throws InstantiationException {
    AllWayMultiInputOutput<String, Integer> oper = new AllWayMultiInputOutput<String, Integer>(1, -1) {
      @Override
      public Integer process(String inputTuple) {
        return null;
      }
    };
  }

  /**
   * Test invalid operator instantiation with positive value for output ports.
   *
   * @throws InstantiationException
   */
  @Test(expected=InstantiationException.class)
  public void testPositiveOutputOperatorInstantiation() throws InstantiationException {
    AllWayMultiInputOutput<String, Integer> oper = new AllWayMultiInputOutput<String, Integer>(1, 65536) {
      @Override
      public Integer process(String inputTuple) {
        return null;
      }
    };
  }

  /**
   * Test invalid operator instantiation with zero value for output ports.
   *
   * @throws InstantiationException
   */
  @Test(expected=InstantiationException.class)
  public void testZeroOutputOperatorInstantiation() throws InstantiationException {
    AllWayMultiInputOutput<String, Integer> oper = new AllWayMultiInputOutput<String, Integer>(1, 0) {
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
    AllWayMultiInputOutput<String, Integer> oper = new AllWayMultiInputOutput<String, Integer>() {
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
   * @param numOutputs number of outputs for the operator
   * @param numValues number of tuples to process for the test
   */
  public void testOperator(int numInputs, int numOutputs, int numValues) throws
      InstantiationException {
    AllWayMultiInputOutput<String, Integer> oper = new AllWayMultiInputOutput<String, Integer>(numInputs, numOutputs) {
      @Override
      public Integer process(String inputTuple) {
        return Integer.parseInt(inputTuple);
      }
    };

    testOperator(oper, numValues);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testOperator(AllWayMultiInputOutput oper, int numValues) {
    int numInputs = oper.inputs.size();
    int numOutputs = oper.outputs.size();

    int[] sumTotals = new int[numOutputs];
    CollectorTestSink[] testSinks = new CollectorTestSink[numOutputs];

    for(int i = 0; i < numOutputs; ++i) {
      testSinks[i] = new CollectorTestSink();
      ((DefaultOutputPort)oper.outputs.get(i)).setSink(testSinks[i]);
    }

    for(int i = 1; i <= numValues; ++i) {
      for(int j = 0; j < numInputs; ++j) {
        ((DefaultInputPort)oper.inputs.get(j)).process(Integer.toString(i));
      }
    }

    for(int i = 0; i < numOutputs; ++i) {
      Assert.assertEquals("number emitted tuples", numValues * numInputs, testSinks[i].collectedTuples.size());

      sumTotals[i] = 0;
      for(Object tuple : testSinks[i].collectedTuples) {
        sumTotals[i] += (Integer)tuple;
      }

      Assert.assertEquals("correct values", ((1 + numValues) * (numValues / 2)) * numInputs, sumTotals[i]);
    }

    for(int i = 0; i < numOutputs; ++i) {
      testSinks[i].clear();
    }
  }
}
