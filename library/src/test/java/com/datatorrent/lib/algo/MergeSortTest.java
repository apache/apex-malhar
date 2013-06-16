/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.algo.MergeSort;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.MergeSort}<p>
 */
public class MergeSortTest
{
  private static Logger log = LoggerFactory.getLogger(MergeSortTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MergeSort<Integer>(), "Integer");
    testNodeProcessingSchema(new MergeSort<Double>(), "Double");
    testNodeProcessingSchema(new MergeSort<Float>(), "Float");
    testNodeProcessingSchema(new MergeSort<String>(), "String");
  }

  public void testNodeProcessingSchema(MergeSort oper, String debug)
  {
    //FirstN<String,Float> aoper = new FirstN<String,Float>();
    TestSink sortSink = new TestSink();
    TestSink hashSink = new TestSink();
    oper.sort.setSink(sortSink);
    oper.sorthash.setSink(hashSink);

    ArrayList input = new ArrayList();

    oper.beginWindow(0);

    input.clear();
    input.add(2);
    oper.data.process(input);

    input.clear();
    input.add(20);
    oper.data.process(input);

    input.clear();
    input.add(1000);
    input.add(5);
    input.add(20);
    input.add(33);
    input.add(33);
    input.add(34);
    oper.data.process(input);

    input.clear();
    input.add(34);
    input.add(1001);
    input.add(6);
    input.add(1);
    input.add(33);
    input.add(9);
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1, hashSink.collectedTuples.size());
    HashMap map = (HashMap) hashSink.collectedTuples.get(0);
    input = (ArrayList) sortSink.collectedTuples.get(0);
    for (Object o: input) {
     log.debug(String.format("%s : %s", o.toString(), map.get(o).toString()));
    }
    log.debug(String.format("Tested %s type with %d tuples and %d uniques\n", debug, input.size(), map.size()));
  }
}
