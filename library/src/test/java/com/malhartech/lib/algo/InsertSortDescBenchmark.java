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
 * Performance tests for {@link com.malhartech.lib.algo.InsertSortDesc}<p>
 */
public class InsertSortDescBenchmark
{
  private static Logger log = LoggerFactory.getLogger(InsertSortDescBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new InsertSortDesc<Integer>(), "Integer");
    testNodeProcessingSchema(new InsertSortDesc<Double>(), "Double");
    testNodeProcessingSchema(new InsertSortDesc<Float>(), "Float");
    testNodeProcessingSchema(new InsertSortDesc<String>(), "String");
  }

  public void testNodeProcessingSchema(InsertSortDesc oper, String debug)
  {
    //FirstN<String,Float> aoper = new FirstN<String,Float>();
    TestSink sortSink = new TestSink();
    oper.sort.setSink(sortSink);
    ArrayList input = new ArrayList();

    int numTuples = 1000000;
    oper.beginWindow(0);

    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.add(numTuples-i);
      oper.datalist.process(input);
      oper.data.process(2);

      input.clear();
      input.add(20);
      oper.datalist.process(input);

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
      input.add(9);
      oper.datalist.process(input);
    }

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    ArrayList list = (ArrayList) sortSink.collectedTuples.get(0);
    log.debug(String.format("Benchmarked %d tuples (%d uniques) of type %s\n", numTuples * 4, list.size(), debug));
  }
}
