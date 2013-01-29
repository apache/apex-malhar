/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestSink;
import com.malhartech.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.algo.UniqueValueKeyVal}<p>
 *
 */
public class UniqueValueKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(UniqueValueKeyValBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    UniqueValueKeyVal<String> oper = new UniqueValueKeyVal<String>();
    TestSink<KeyValPair<String,Integer>> sink = new TestSink<KeyValPair<String,Integer>>();
    oper.count.setSink(sink);

    KeyValPair<String,Integer> atuple = new KeyValPair("a", 1);
    KeyValPair<String,Integer> btuple = new KeyValPair("b", 1);
    KeyValPair<String,Integer> ctuple = new KeyValPair("c", 5);
    KeyValPair<String,Integer> dtuple = new KeyValPair("d", 2);
    KeyValPair<String,Integer> e1tuple = new KeyValPair("e", 5);
    KeyValPair<String,Integer> e2tuple = new KeyValPair("e", 2);

    int numTuples = 10000000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      atuple.setValue(i);
      oper.data.process(atuple);
      if (i % 2 == 0) {
        oper.data.process(btuple);
        oper.data.process(e2tuple);
      }
      if (i % 3 == 0) {
        oper.data.process(ctuple);
      }
      if (i % 5 == 0) {
        oper.data.process(dtuple);
      }
      if (i % 10 == 0) {
        oper.data.process(e1tuple);
      }
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 5, sink.collectedTuples.size());
    log.debug(String.format("\nBenchmark sums for %d key/val pairs", numTuples * 6));

    for (KeyValPair<String,Integer> e : sink.collectedTuples) {
      int val = e.getValue().intValue();
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", numTuples, val);
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", 1, val);
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", 1, val);
      }
      else if (e.getKey().equals("d")) {
        Assert.assertEquals("emitted tuple for 'd' was ", 1, val);
      }
      else if (e.getKey().equals("e")) {
        Assert.assertEquals("emitted tuple for 'e' was ", 2, val);
      }
    }
  }
}
