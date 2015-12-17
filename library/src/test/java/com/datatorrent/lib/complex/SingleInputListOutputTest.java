package com.datatorrent.lib.complex;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.testbench.CollectorTestSink;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SingleInputListOutputTest {
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception {
    List<Integer> outputSizes =
        new ArrayList<Integer>(Arrays.asList(SingleInputListOutput.DEFAULT_NUM_OUTPUTS, 10, 100));
    List<Integer> valueSizes =
        new ArrayList<Integer>(Arrays.asList(10, 1000));

    // Test against the default constructor
    testOperator(valueSizes.get(0));

    // Test against the test matrix for this operator
    for(Integer output : outputSizes) {
      for(Integer value : valueSizes) {
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
    SingleInputListOutput<String, Integer> oper = new SingleInputListOutput<String, Integer>(-1) {
      @Override
      public List<Integer> process(String inputTuple) {
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
    SingleInputListOutput<String, Integer> oper = new SingleInputListOutput<String, Integer>(65536) {
      @Override
      public List<Integer> process(String inputTuple) {
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
    SingleInputListOutput<String, Integer> oper = new SingleInputListOutput<String, Integer>(0) {
      @Override
      public List<Integer> process(String inputTuple) {
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
    SingleInputListOutput<String, Integer> oper = new SingleInputListOutput<String, Integer>() {
      List<Integer> results = new ArrayList<Integer>(this.outputs.size());

      @Override
      public List<Integer> process(String inputTuple) {
        results.clear();

        for (int i = 0; i < this.outputs.size(); ++i) {
          results.add(i, Integer.parseInt(inputTuple) * i);
        }

        return results;
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
  public void testOperator(int numOutputs, int numValues) throws InstantiationException {
    SingleInputListOutput<String, Integer> oper = new SingleInputListOutput<String, Integer>(numOutputs) {
      List<Integer> results = new ArrayList<Integer>(this.outputs.size());

      @Override
      public List<Integer> process(String inputTuple) {
        results.clear();

        for (int i = 0; i < this.outputs.size(); ++i) {
          results.add(i, Integer.parseInt(inputTuple) * i);
        }

        return results;
      }
    };

    testOperator(oper, numValues);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testOperator(SingleInputListOutput oper, int numValues) {
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

      Assert.assertEquals("correct values", (i + (numValues * i)) * (numValues / 2), sumTotals[i]);
    }

    for(int i = 0; i < numOutputs; ++i) {
      testSinks[i].clear();
    }
  }
}
