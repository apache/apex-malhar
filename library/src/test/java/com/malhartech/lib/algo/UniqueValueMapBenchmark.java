/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestSink;
import com.malhartech.lib.util.KeyValPair;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.UniqueValueKeyVal}<p>
 *
 */
public class UniqueValueMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(UniqueValueMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    UniqueValueMap<String> oper = new UniqueValueMap<String>();
    TestSink<HashMap<String,Integer>> sink = new TestSink<HashMap<String,Integer>>();
    oper.count.setSink(sink);

    HashMap<String, Integer> h1 = new HashMap<String, Integer>();
    h1.put("a", 1);
    h1.put("b", 1);

    HashMap<String,Integer> h2 = new HashMap<String, Integer>();
    h2.put("a", 2);
    h2.put("c", 5);
    h2.put("e", 5);

    HashMap<String,Integer> h3 = new HashMap<String, Integer>();
    h3.put("d", 2);
    h3.put("e", 2);

    int numTuples = 10000000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      h1.put("a", i);
      oper.data.process(h1);
      if (i % 2 == 0) {
        oper.data.process(h2);
      }
      if (i % 3 == 0) {
        oper.data.process(h3);
      }

    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());
    HashMap<String, Integer> e = (HashMap<String,Integer>) sink.collectedTuples.get(0);
    log.debug(String.format("\nBenchmark sums for %d key/val pairs", numTuples * 3));
    Assert.assertEquals("emitted value for 'a' was ", numTuples, e.get("a").intValue());
    Assert.assertEquals("emitted value for 'b' was ", 1, e.get("b").intValue());
    Assert.assertEquals("emitted value for 'c' was ", 1, e.get("c").intValue());
    Assert.assertEquals("emitted value for 'd' was ", 1, e.get("d").intValue());
    Assert.assertEquals("emitted value for 'e' was ", 2, e.get("e").intValue());
  }
}
