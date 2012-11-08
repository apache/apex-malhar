/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.LeastFrequentKeyValue}<p>
 *
 */
public class LeastFrequentKeyValueTest
{
  private static Logger log = LoggerFactory.getLogger(LeastFrequentKeyValueTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    LeastFrequentKeyValue<String, Integer> oper = new LeastFrequentKeyValue<String, Integer>();
    TestSink matchSink = new TestSink();
    oper.least.setSink(matchSink);

    oper.beginWindow(0);
    HashMap<String, Integer> amap = new HashMap<String, Integer>(1);
    HashMap<String, Integer> bmap = new HashMap<String, Integer>(1);
    HashMap<String, Integer> cmap = new HashMap<String, Integer>(1);
    int atot1 = 5;
    int btot1 = 3;
    int ctot1 = 6;
    amap.put("a", 1);
    bmap.put("b", 2);
    cmap.put("c", 4);
    for (int i = 0; i < atot1; i++) {
      oper.data.process(amap);
    }
    for (int i = 0; i < btot1; i++) {
      oper.data.process(bmap);
    }
    for (int i = 0; i < ctot1; i++) {
      oper.data.process(cmap);
    }

    atot1 = 4;
    btot1 = 3;
    ctot1 = 10;
    amap.put("a", 5);
    bmap.put("b", 4);
    cmap.put("c", 3);
    for (int i = 0; i < atot1; i++) {
      oper.data.process(amap);
    }
    for (int i = 0; i < btot1; i++) {
      oper.data.process(bmap);
    }
    for (int i = 0; i < ctot1; i++) {
      oper.data.process(cmap);
    }

    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 3, matchSink.collectedTuples.size());
    int vcount;
    for (Object o: matchSink.collectedTuples) {
      HashMap<String, HashMap<Integer, Integer>> omap = (HashMap<String, HashMap<Integer, Integer>>)o;
      for (Map.Entry<String, HashMap<Integer, Integer>> e: omap.entrySet()) {
        String key = e.getKey();
        if (key.equals("a")) {
          vcount = e.getValue().get(5);
          Assert.assertEquals("Key \"a\" has value ", 4, vcount);
        }
        else if (key.equals("b")) {
          vcount = e.getValue().get(2);
          Assert.assertEquals("Key \"a\" has value ", 3, vcount);
          vcount = e.getValue().get(4);
          Assert.assertEquals("Key \"a\" has value ", 3, vcount);
        }
        else if (key.equals("c")) {
          vcount = e.getValue().get(4);
          Assert.assertEquals("Key \"a\" has value ", 6, vcount);
        }
      }
    }
  }
}
