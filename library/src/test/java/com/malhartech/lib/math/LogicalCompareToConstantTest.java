package com.malhartech.lib.math;

/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */


import com.malhartech.engine.TestSink;
import com.malhartech.util.Pair;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.LogicalCompareToConstant}<p>
 *
 */
public class LogicalCompareToConstantTest
{
  private static Logger log = LoggerFactory.getLogger(LogicalCompareToConstantTest.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeProcessing()
  {
    LogicalCompareToConstant<Integer> oper = new LogicalCompareToConstant<Integer>() {};
    TestSink eSink = new TestSink();
    TestSink neSink = new TestSink();
    TestSink gtSink = new TestSink();
    TestSink gteSink = new TestSink();
    TestSink ltSink = new TestSink();
    TestSink lteSink = new TestSink();

    oper.equalTo.setSink(eSink);
    oper.notEqualTo.setSink(neSink);
    oper.greaterThan.setSink(gtSink);
    oper.greaterThanOrEqualTo.setSink(gteSink);
    oper.lessThan.setSink(ltSink);
    oper.lessThanOrEqualTo.setSink(lteSink);
    oper.setConstant(2);

    oper.beginWindow(0); //
    oper.input.process(1);
    oper.input.process(2);
    oper.input.process(3);

    oper.endWindow(); //

    Assert.assertEquals("number emitted tuples", 1, eSink.collectedTuples.size());
    Assert.assertEquals("tuples were", eSink.collectedTuples.get(0).equals(2), true);

    Assert.assertEquals("number emitted tuples", 2, neSink.collectedTuples.size());
    Assert.assertEquals("tuples were", neSink.collectedTuples.get(0).equals(1), true);
    Assert.assertEquals("tuples were", neSink.collectedTuples.get(1).equals(3), true);

    Assert.assertEquals("number emitted tuples", 1, gtSink.collectedTuples.size());
    Assert.assertEquals("tuples were", gtSink.collectedTuples.get(0).equals(1), true);

    Assert.assertEquals("number emitted tuples", 2, gteSink.collectedTuples.size());
    Assert.assertEquals("tuples were",gteSink.collectedTuples.get(0).equals(1), true);
    Assert.assertEquals("tuples were", gteSink.collectedTuples.get(1).equals(2), true);

    Assert.assertEquals("number emitted tuples", 1, ltSink.collectedTuples.size());
    Assert.assertEquals("tuples were", ltSink.collectedTuples.get(0).equals(3), true);

    Assert.assertEquals("number emitted tuples", 2, lteSink.collectedTuples.size());
    Assert.assertEquals("tuples were", lteSink.collectedTuples.get(0).equals(2), true);
    Assert.assertEquals("tuples were", lteSink.collectedTuples.get(1).equals(3), true);
  }
}
