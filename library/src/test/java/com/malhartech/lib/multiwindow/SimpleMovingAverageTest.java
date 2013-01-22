/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.multiwindow;

import com.malhartech.engine.TestSink;
import com.malhartech.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.malhartech.lib.multiwindow.SimpleMovingAverage}<p>
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class SimpleMovingAverageTest
{
  private static final Logger logger = LoggerFactory.getLogger(SimpleMovingAverageTest.class);

  /**
   * Test functional logic
   */
  @Test
  public void testNodeProcessing() throws InterruptedException
  {
    SimpleMovingAverage<String, Double> oper = new SimpleMovingAverage<String, Double>();

    TestSink<KeyValPair<String, Double>> sink = new TestSink<KeyValPair<String, Double>>();
    TestSink<KeyValPair<String, Integer>> sink2 = new TestSink<KeyValPair<String, Integer>>();
    oper.doubleSMA.setSink(sink);
    oper.integerSMA.setSink(sink2);
    oper.setWindowSize(3);

    double val = 30;
    double val2 = 51;
    oper.beginWindow(0);
    oper.data.process(new KeyValPair("a", ++val));
    oper.data.process(new KeyValPair("a", ++val));
    oper.data.process(new KeyValPair("b", ++val2));
    oper.data.process(new KeyValPair("b", ++val2));
    oper.endWindow();

    oper.beginWindow(1);
    oper.data.process(new KeyValPair("a", ++val));
    oper.data.process(new KeyValPair("a", ++val));
    oper.data.process(new KeyValPair("b", ++val2));
    oper.data.process(new KeyValPair("b", ++val2));
    oper.endWindow();

    oper.beginWindow(2);
    oper.data.process(new KeyValPair("a", ++val));
    oper.data.process(new KeyValPair("a", ++val));
    oper.data.process(new KeyValPair("b", ++val2));
    oper.data.process(new KeyValPair("b", ++val2));
    oper.endWindow();

    oper.beginWindow(3);
    oper.data.process(new KeyValPair("a", ++val));
    oper.data.process(new KeyValPair("a", ++val));
    oper.data.process(new KeyValPair("b", ++val2));
    oper.data.process(new KeyValPair("b", ++val2));
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 8, sink.collectedTuples.size());
    Assert.assertEquals("1st sma", 52.5, sink.collectedTuples.get(0).getValue().doubleValue());
    Assert.assertEquals("2nd sma", 53.5, sink.collectedTuples.get(2).getValue().doubleValue());
    Assert.assertEquals("3rd sma", 54.5, sink.collectedTuples.get(4).getValue().doubleValue());
    Assert.assertEquals("4th sma", 56.5, sink.collectedTuples.get(6).getValue().doubleValue());
    Assert.assertEquals("1st sma", 52, sink2.collectedTuples.get(0).getValue().intValue());

  }
}
