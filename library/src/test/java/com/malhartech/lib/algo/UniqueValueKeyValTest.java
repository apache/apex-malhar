/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestSink;
import com.malhartech.common.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.UniqueValueKeyVal}<p>
 *
 */
public class UniqueValueKeyValTest
{
  private static Logger log = LoggerFactory.getLogger(UniqueValueKeyValTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    UniqueValueKeyVal<String> oper = new UniqueValueKeyVal<String>();
    TestSink sink = new TestSink();
    oper.count.setSink(sink);

    KeyValPair<String,Integer> a1tuple = new KeyValPair("a", 1);
    KeyValPair<String,Integer> a2tuple = new KeyValPair("a", 2);
    KeyValPair<String,Integer> btuple = new KeyValPair("b", 1);
    KeyValPair<String,Integer> ctuple = new KeyValPair("c", 5);
    KeyValPair<String,Integer> dtuple = new KeyValPair("d", 2);
    KeyValPair<String,Integer> e1tuple = new KeyValPair("e", 5);
    KeyValPair<String,Integer> e2tuple = new KeyValPair("e", 2);

    int numTuples = 10000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(a1tuple);
      if (i % 2 == 0) {
        oper.data.process(btuple);
        oper.data.process(e2tuple);
      }
      if (i % 3 == 0) {
        oper.data.process(ctuple);
      }
      if (i % 5 == 0) {
        oper.data.process(dtuple);
        oper.data.process(a2tuple);
      }
      if (i % 10 == 0) {
        oper.data.process(e1tuple);
      }
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 5, sink.collectedTuples.size());
    for (Object o : sink.collectedTuples) {
      KeyValPair<String,Integer> e = (KeyValPair<String,Integer>)o;
      int val = e.getValue().intValue();
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", 2, val);
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
