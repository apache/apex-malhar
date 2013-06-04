/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestSink;
import com.malhartech.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.CountKeyVal}. <p>
 *
 */
public class CountKeyValTest
{
  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeProcessing()
  {
    CountKeyVal<String, Double> oper = new CountKeyVal<String, Double>();
    TestSink countSink = new TestSink();
    oper.count.setSink(countSink);

    oper.beginWindow(0); //

    oper.data.process(new KeyValPair("a", 2.0));
    oper.data.process(new KeyValPair("b", 20.0));
    oper.data.process(new KeyValPair("c", 1000.0));
    oper.data.process(new KeyValPair("a", 1.0));
    oper.data.process(new KeyValPair("a", 10.0));
    oper.data.process(new KeyValPair("b", 5.0));
    oper.data.process(new KeyValPair("d", 55.0));
    oper.data.process(new KeyValPair("b", 12.0));
    oper.data.process(new KeyValPair("d", 22.0));
    oper.data.process(new KeyValPair("d", 14.2));
    oper.data.process(new KeyValPair("d", 46.0));
    oper.data.process(new KeyValPair("e", 2.0));
    oper.data.process(new KeyValPair("a", 23.0));
    oper.data.process(new KeyValPair("d", 4.0));

    oper.endWindow(); //

    // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
    Assert.assertEquals("number emitted tuples", 5, countSink.collectedTuples.size());
    for (Object o: countSink.collectedTuples) {
      KeyValPair<String, Integer> e = (KeyValPair<String, Integer>)o;
      Integer val = (Integer)e.getValue();
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", 4, val.intValue());
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", 3, val.intValue());
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", 1, val.intValue());
      }
      else if (e.getKey().equals("d")) {
        Assert.assertEquals("emitted tuple for 'd' was ", 5, val.intValue());
      }
      else if (e.getKey().equals("e")) {
        Assert.assertEquals("emitted tuple for 'e' was ", 1, val.intValue());
      }
    }
  }
}
