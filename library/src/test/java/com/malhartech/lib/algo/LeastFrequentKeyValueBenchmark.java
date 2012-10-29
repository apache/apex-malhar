/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestSink;
import com.malhartech.lib.util.MutableInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.LeastFrequentKeyValue}<p>
 *
 */
public class LeastFrequentKeyValueBenchmark
{
  private static Logger log = LoggerFactory.getLogger(LeastFrequentKeyValueBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    LeastFrequentKeyValue<String, Integer> oper = new LeastFrequentKeyValue<String, Integer>();
    TestSink matchSink = new TestSink();
    oper.least.setSink(matchSink);
    oper.setup(new OperatorConfiguration());

    oper.beginWindow();
    HashMap<String, Integer> amap = new HashMap<String, Integer>(1);
    HashMap<String, Integer> bmap = new HashMap<String, Integer>(1);
    HashMap<String, Integer> cmap = new HashMap<String, Integer>(1);
    int atot1 = 0;
    int btot1 = 0;
    int ctot1 = 0;
    int atot2 = 0;
    int btot2 = 0;
    int ctot2 = 0;
    int numTuples = 10000000;
    for (int j = 0; j < numTuples; j++) {
      atot1 = 5;
      btot1 = 3;
      ctot1 = 6;
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

      atot2 = 4;
      btot2 = 3;
      ctot2 = 10;
      amap.put("a", 5);
      bmap.put("b", 4);
      cmap.put("c", 3);
      for (int i = 0; i < atot2; i++) {
        oper.data.process(amap);
      }
      for (int i = 0; i < btot2; i++) {
        oper.data.process(bmap);
      }
      for (int i = 0; i < ctot2; i++) {
        oper.data.process(cmap);
      }
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
          Assert.assertEquals("Key \"a\" has value ", numTuples * 4, vcount);
        }
        else if (key.equals("b")) {
          vcount = e.getValue().get(2);
          Assert.assertEquals("Key \"a\" has value ", numTuples * 3, vcount);
          vcount = e.getValue().get(4);
          Assert.assertEquals("Key \"a\" has value ", numTuples * 3, vcount);
        }
        else if (key.equals("c")) {
          vcount = e.getValue().get(4);
          Assert.assertEquals("Key \"a\" has value ", numTuples * 6, vcount);
        }
      }
    }
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * (atot1 + atot2 + btot1 + btot2 + ctot1 + ctot2)));
  }
}