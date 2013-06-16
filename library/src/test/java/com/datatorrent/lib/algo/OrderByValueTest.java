/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.algo.OrderByValue;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.OrderByValue}<p>
 */
public class OrderByValueTest
{
  private static Logger log = LoggerFactory.getLogger(OrderByValueTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    OrderByValue<String, Integer> oper = new OrderByValue<String, Integer>();
    TestSink countSink = new TestSink();
    TestSink listSink = new TestSink();
    oper.ordered_list.setSink(countSink);
    oper.ordered_count.setSink(listSink);
    String[] keys = new String[2];
    keys[0] = "a";
    keys[1] = "b";
    oper.setFilterBy(keys);
    oper.setInverse(false);
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
    input.put("b", -50);
    input.put("c", 6);
    oper.data.process(input);

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 14, countSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 10, listSink.collectedTuples.size());

    log.debug("\ncount port tuples");
    int tval = 0;
    int pval = -100;
    for (Object e: countSink.collectedTuples) {
      HashMap<String, Integer> tuple = (HashMap<String, Integer>)e;
      for (Map.Entry<String, Integer> o: tuple.entrySet()) {
        int oval = o.getValue().intValue();
        if (oval < pval) {
          Assert.assertEquals("order is messed up", 0, 1);
        }
        pval = oval;
        tval += oval;
      }
    }
    Assert.assertEquals("order is messed up", 55, tval);

    tval = 0;
    int cval = 0;
    pval = -100;
    for (Object e: listSink.collectedTuples) {
      HashMap<String, HashMap<Integer, Integer>> tuple = (HashMap<String, HashMap<Integer, Integer>>)e;
      for (Map.Entry<String, HashMap<Integer, Integer>> o: tuple.entrySet()) {
        for (Map.Entry<Integer, Integer> t: o.getValue().entrySet()) {
          int oval = t.getKey().intValue();
          if (oval < pval) {
            Assert.assertEquals("order is messed up", 0, 1);
          }
          pval = oval;
          tval += oval;
          cval += t.getValue().intValue();
        }
      }
    }
    Assert.assertEquals("order is messed up", 42, tval);
    Assert.assertEquals("order is messed up", 14, cval);
  }
}
