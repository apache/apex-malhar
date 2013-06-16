/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.algo.OrderByKey;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.OrderByKey}<p>
 */
public class OrderByKeyTest
{
  private static Logger log = LoggerFactory.getLogger(OrderByKeyTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    OrderByKey<String, Integer> oper = new OrderByKey<String, Integer>();
    TestSink countSink = new TestSink();
    TestSink listSink = new TestSink();
    oper.ordered_count.setSink(countSink);
    oper.ordered_list.setSink(listSink);
    oper.setOrderby("a");
    oper.setup(null);

    HashMap<String, Integer> input = new HashMap<String, Integer>();

    oper.beginWindow(0);
    input.clear();
    input.put("a", 2);
    input.put("b", 5);
    input.put("c", 6);
    oper.data.process(input);

    input.clear();
    input.put("b", 50);
    input.put("c", 16);
    oper.data.process(input);

    input.clear();
    input.put("a", 1);
    input.put("b", 2);
    input.put("c", 3);
    oper.data.process(input);

    input.clear();
    input.put("a", 1);
    input.put("b", 7);
    input.put("c", 4);
    oper.data.process(input);

    input.clear();
    input.put("a", 3);
    input.put("b", 23);
    input.put("c", 33);
    oper.data.process(input);

    input.clear();
    input.put("a", 2);
    input.put("b", 5);
    input.put("c", 6);
    oper.data.process(input);

    input.clear();
    input.put("e", 2);
    input.put("b", 5);
    input.put("c", 6);
    oper.data.process(input);

    input.clear();
    input.put("a", -1);
    input.put("b", -5);
    input.put("c", 6);
    oper.data.process(input);

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 4, countSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 6, listSink.collectedTuples.size());

    int state = -1;
    for (Object e: countSink.collectedTuples) {
      log.debug(e.toString());
      HashMap<Integer, Integer> map = (HashMap<Integer, Integer>)e;
      if (map.get(-1) != null) {
        Assert.assertEquals("count of -1", 1, map.get(-1).intValue());
        Assert.assertEquals("order should be", -1, state);
        state = 1;
      }
      else if (map.get(1) != null) {
        Assert.assertEquals("count of 1", 2, map.get(1).intValue());
        Assert.assertEquals("order should be", 1, state);
        state = 2;
      }
      else if (map.get(2) != null) {
        Assert.assertEquals("count of 2", 2, map.get(2).intValue());
        Assert.assertEquals("order should be", 2, state);
        state = 3;
      }
      else if (map.get(3) != null) {
        Assert.assertEquals("count of 3", 1, map.get(3).intValue());
        Assert.assertEquals("order should be", 3, state);
      }
    }

    state = -1;
    int jump = 0;
    for (Object e: listSink.collectedTuples) {
      log.debug(e.toString());
      HashMap<String, Integer> map = (HashMap<String, Integer>) e;
      int aval = map.get("a").intValue();
      if (aval == -1) {
        Assert.assertEquals("order should be", -1, state);
        state = 1;
      }
      else if (aval == 1) {
        Assert.assertEquals("order should be", 1, state);
        if (jump == 1) {
          state = 2;
        }
        jump++;
      }
      else if (aval == 2) {
        Assert.assertEquals("order should be", 2, state);
        if (jump == 3) {
          state = 3;
        }
        jump++;
      }
      else if (aval == 3) {
        Assert.assertEquals("order should be", 3, state);
      }
      else {
        Assert.assertEquals("wrong tuple", 0, 1);
      }
    }
  }
}
