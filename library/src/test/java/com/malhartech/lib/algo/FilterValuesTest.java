/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestSink;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.FilterValues}<p>
 *
 */
public class FilterValuesTest
{
  int getTotal(List list)
  {
    ArrayList<Integer> ilist = (ArrayList<Integer>) list;
    int ret = 0;
    for (Integer i : ilist) {
      ret += i.intValue();
    }
    return ret;
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    FilterValues<Integer> oper = new FilterValues<Integer>();

    TestSink<Integer> sortSink = new TestSink<Integer>();
    oper.filter.setSink(sortSink);
    ArrayList<Integer> Values = new ArrayList<Integer>();
    oper.setValue(5);
    oper.clearValues();
    Values.add(200);
    Values.add(2);
    oper.setValue(4);
    oper.setValues(Values);

    oper.beginWindow(0);
    oper.data.process( 2);
    oper.data.process(5);
    oper.data.process(7);
    oper.data.process(42);
    oper.data.process(200);
    oper.data.process(2);
    Assert.assertEquals("number emitted tuples", 3, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 204, getTotal(sortSink.collectedTuples));
    sortSink.clear();

    oper.data.process(5);
    Assert.assertEquals("number emitted tuples", 0, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 0, getTotal(sortSink.collectedTuples));
    sortSink.clear();

    oper.data.process(2);
    oper.data.process(33);
    oper.data.process(2);
    Assert.assertEquals("number emitted tuples", 2, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 4, getTotal(sortSink.collectedTuples));
    sortSink.clear();

    oper.data.process(6);
    oper.data.process(2);
    oper.data.process(6);
    oper.data.process(2);
    oper.data.process(6);
    oper.data.process(2);
    oper.data.process(6);
    oper.data.process(2);
    Assert.assertEquals("number emitted tuples", 4, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 8, getTotal(sortSink.collectedTuples));
    sortSink.clear();

    oper.setInverse(true);
    oper.data.process(9);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 9, getTotal(sortSink.collectedTuples));

    oper.endWindow();
  }
}
