/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.FilterKeysMap;
import com.malhartech.engine.TestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.FilterKeysMap}<p>
 *
 */
public class FilterKeysMapTest
{
  int getTotal(Object o)
  {
    HashMap<String, Number> map = (HashMap<String, Number>)o;
    int ret = 0;
    for (Map.Entry<String, Number> e: map.entrySet()) {
      ret += e.getValue().intValue();
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
    FilterKeysMap<String,Number> oper = new FilterKeysMap<String,Number>();

    TestSink sortSink = new TestSink();
    oper.filter.setSink(sortSink);
    oper.setKey("b");
    oper.clearKeys();
    String [] keys = new String[3];
    keys[0] = "e";
    keys[1] = "f";
    keys[2] = "blah";
    oper.setKey("a");
    oper.setKeys(keys);

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();

    input.put("a", 2);
    input.put("b", 5);
    input.put("c", 7);
    input.put("d", 42);
    input.put("e", 200);
    input.put("f", 2);
    oper.data.process(input);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 204, getTotal(sortSink.collectedTuples.get(0)));
    sortSink.clear();

    input.clear();
    input.put("a", 5);
    oper.data.process(input);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 5, getTotal(sortSink.collectedTuples.get(0)));
    sortSink.clear();

    input.clear();
    input.put("a", 2);
    input.put("b", 33);
    input.put("f", 2);
    oper.data.process(input);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 4, getTotal(sortSink.collectedTuples.get(0)));
    sortSink.clear();

    input.clear();
    input.put("b", 6);
    input.put("a", 2);
    input.put("j", 6);
    input.put("e", 2);
    input.put("dd", 6);
    input.put("blah", 2);
    input.put("another", 6);
    input.put("notmakingit", 2);
    oper.data.process(input);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 6, getTotal(sortSink.collectedTuples.get(0)));
    sortSink.clear();

    input.clear();
    input.put("c", 9);
    oper.setInverse(true);
    oper.data.process(input);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 9, getTotal(sortSink.collectedTuples.get(0)));

    oper.endWindow();
  }
}
