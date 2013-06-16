/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.InsertSort}<p>
 */
public class InsertSortTest
{
  private static Logger log = LoggerFactory.getLogger(InsertSortTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new InsertSort<Integer>(), "Integer");
    testNodeProcessingSchema(new InsertSort<Double>(), "Double");
    testNodeProcessingSchema(new InsertSort<Float>(), "Float");
    testNodeProcessingSchema(new InsertSort<String>(), "String");
  }

  public void testNodeProcessingSchema(InsertSort oper, String debug)
  {
    //FirstN<String,Float> aoper = new FirstN<String,Float>();
    TestSink sortSink = new TestSink();
    TestSink hashSink = new TestSink();
    oper.sort.setSink(sortSink);
    oper.sorthash.setSink(hashSink);

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
    Assert.assertEquals("number emitted tuples", 1, hashSink.collectedTuples.size());
    HashMap map = (HashMap) hashSink.collectedTuples.get(0);
    input = (ArrayList) sortSink.collectedTuples.get(0);
    for (Object o: input) {
     log.debug(String.format("%s : %s", o.toString(), map.get(o).toString()));
    }
    log.debug(String.format("Tested %s type with %d tuples and %d uniques\n", debug, input.size(), map.size()));
  }
}
