/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.math.MarginKeyVal;
import com.datatorrent.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.MarginKeyVal}. <p>
 *
 */
public class MarginKeyValTest
{
  /**
   * Test node logic emits correct results.
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MarginKeyVal<String, Integer>());
    testNodeProcessingSchema(new MarginKeyVal<String, Double>());
    testNodeProcessingSchema(new MarginKeyVal<String, Float>());
    testNodeProcessingSchema(new MarginKeyVal<String, Short>());
    testNodeProcessingSchema(new MarginKeyVal<String, Long>());
  }

  public void testNodeProcessingSchema(MarginKeyVal oper)
  {
    TestSink marginSink = new TestSink();

    oper.margin.setSink(marginSink);

    oper.beginWindow(0);
    oper.numerator.process(new KeyValPair("a", 2));
    oper.numerator.process(new KeyValPair("b", 20));
    oper.numerator.process(new KeyValPair("c", 1000));

    oper.denominator.process(new KeyValPair("a", 2));
    oper.denominator.process(new KeyValPair("b", 40));
    oper.denominator.process(new KeyValPair("c", 500));
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3, marginSink.collectedTuples.size());
    for (int i = 0; i < marginSink.collectedTuples.size(); i++) {
      if ("a".equals(((KeyValPair<String, Number>)marginSink.collectedTuples.get(i)).getKey())) {
        Assert.assertEquals("emitted value for 'a' was ", new Double(0), ((KeyValPair<String, Number>)marginSink.collectedTuples.get(i)).getValue().doubleValue());
      }
      if ("b".equals(((KeyValPair<String, Number>)marginSink.collectedTuples.get(i)).getKey())) {
        Assert.assertEquals("emitted value for 'b' was ", new Double(0.5), ((KeyValPair<String, Number>)marginSink.collectedTuples.get(i)).getValue().doubleValue());
      }
      if ("c".equals(((KeyValPair<String, Number>)marginSink.collectedTuples.get(i)).getKey())) {
        Assert.assertEquals("emitted value for 'c' was ", new Double(-1), ((KeyValPair<String, Number>)marginSink.collectedTuples.get(i)).getValue().doubleValue());
      }
    }
  }
}