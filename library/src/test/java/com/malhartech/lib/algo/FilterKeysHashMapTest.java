/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.FilterKeysHashMap}<p>
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class FilterKeysHashMapTest
{
  int getTotal(Object o)
  {
    HashMap<String, HashMap<String, Number>> map = (HashMap<String, HashMap<String, Number>>)o;
    int ret = 0;
    for (Map.Entry<String, HashMap<String, Number>> e: map.entrySet()) {
      for (Map.Entry<String, Number> e2: e.getValue().entrySet()) {
        ret += e2.getValue().intValue();
      }
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
    FilterKeysHashMap<String, Number> oper = new FilterKeysHashMap<String, Number>();

    TestSink<HashMap<String, HashMap<String, Number>>> sortSink = new TestSink<HashMap<String, HashMap<String, Number>>>();
    oper.filter.setSink(sortSink);
    oper.setKey("b");
    oper.clearKeys();
    String[] keys = new String[3];
    keys[0] = "e";
    keys[1] = "f";
    keys[2] = "blah";
    oper.setKey("a");
    oper.setKeys(keys);

    oper.beginWindow(0);
    HashMap<String, HashMap<String, Number>> inputA = new HashMap<String, HashMap<String, Number>>();
    HashMap<String, Number> input = new HashMap<String, Number>();
    HashMap<String, Number> input2 = new HashMap<String, Number>();

    input.put("a", 2);
    input.put("b", 5);
    input.put("c", 7);
    input.put("d", 42);
    input.put("e", 200);
    input.put("f", 2);
    inputA.put("A", input);
    oper.data.process(inputA);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 204, getTotal(sortSink.collectedTuples.get(0)));
    sortSink.clear();

    input.clear();
    inputA.clear();
    input.put("a", 5);
    inputA.put("A", input);
    oper.data.process(inputA);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 5, getTotal(sortSink.collectedTuples.get(0)));
    sortSink.clear();

    input.clear();
    inputA.clear();
    input.put("a", 2);
    input.put("b", 33);
    input.put("f", 2);
    inputA.put("A", input);
    oper.data.process(inputA);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 4, getTotal(sortSink.collectedTuples.get(0)));
    sortSink.clear();

    input.clear();
    inputA.clear();
    input.put("b", 6);
    input.put("a", 2);
    input.put("j", 6);
    input.put("e", 2);
    input.put("dd", 6);
    input.put("blah", 2);
    input.put("another", 6);
    input.put("notmakingit", 2);
    inputA.put("A", input);
    oper.data.process(inputA);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 6, getTotal(sortSink.collectedTuples.get(0)));
    sortSink.clear();

    input.clear();
    inputA.clear();
    input.put("c", 9);
    oper.setInverse(true);
    inputA.put("A", input);
    oper.data.process(inputA);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 9, getTotal(sortSink.collectedTuples.get(0)));
    sortSink.clear();

    input.clear();
    input2.clear();
    inputA.clear();
    input.put("e", 2); // pass
    input.put("c", 9);
    input2.put("a", 5); // pass
    input2.put("p", 8);
    oper.setInverse(false);
    inputA.put("A", input);
    inputA.put("B", input2);
    oper.data.process(inputA);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 7, getTotal(sortSink.collectedTuples.get(0)));
    sortSink.clear();

    oper.endWindow();
  }
}
