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
