/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestSink;
import com.malhartech.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.AverageKeyVal}. <p>
 *
 */
public class AverageKeyValTest
{
  private static Logger LOG = LoggerFactory.getLogger(SumMap.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeProcessing()
  {
    AverageKeyVal<String, Double> oper = new AverageKeyVal<String, Double>();
    oper.setType(Double.class);
    TestSink averageSink = new TestSink();

    oper.average.setSink(averageSink);

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

    Assert.assertEquals("number emitted tuples", 5, averageSink.collectedTuples.size());
    for (Object o: averageSink.collectedTuples) {
      KeyValPair<String, Double> e = (KeyValPair<String, Double>)o;
      Double val = e.getValue();
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Double(36 / 4.0), val);
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", new Double(37 / 3.0), val);
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", new Double(1000 / 1.0), val);
      }
      else if (e.getKey().equals("d")) {
        Assert.assertEquals("emitted tuple for 'd' was ", new Double(141.2 / 5), val);
      }
      else if (e.getKey().equals("e")) {
        Assert.assertEquals("emitted tuple for 'e' was ", new Double(2 / 1.0), val);
      }
    }
  }
}
