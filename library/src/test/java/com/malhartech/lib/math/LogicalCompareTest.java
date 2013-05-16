package com.malhartech.lib.math;

/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
import com.malhartech.engine.TestSink;
import com.malhartech.common.Pair;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.LogicalCompare}<p>
 *
 */
public class LogicalCompareTest
{
  private static Logger log = LoggerFactory.getLogger(LogicalCompareTest.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeProcessing()
  {
    LogicalCompare<Integer> oper = new LogicalCompare<Integer>()
    {
    };
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

    Pair<Integer, Integer> gtuple = new Pair(2, 1);
    Pair<Integer, Integer> etuple = new Pair(2, 2);
    Pair<Integer, Integer> ltuple = new Pair(2, 3);

    oper.beginWindow(0); //

    oper.input.process(gtuple);
    oper.input.process(etuple);
    oper.input.process(ltuple);

    oper.endWindow(); //

    Assert.assertEquals("number emitted tuples", 1, eSink.collectedTuples.size());
    Assert.assertEquals("tuples were", eSink.collectedTuples.get(0).equals(etuple), true);

    Assert.assertEquals("number emitted tuples", 2, neSink.collectedTuples.size());
    Assert.assertEquals("tuples were", neSink.collectedTuples.get(0).equals(gtuple), true);
    Assert.assertEquals("tuples were", neSink.collectedTuples.get(1).equals(ltuple), true);

    Assert.assertEquals("number emitted tuples", 1, gtSink.collectedTuples.size());
    Assert.assertEquals("tuples were", gtSink.collectedTuples.get(0).equals(gtuple), true);

    Assert.assertEquals("number emitted tuples", 2, gteSink.collectedTuples.size());
    Assert.assertEquals("tuples were", gteSink.collectedTuples.get(0).equals(gtuple), true);
    Assert.assertEquals("tuples were", gteSink.collectedTuples.get(1).equals(etuple), true);

    Assert.assertEquals("number emitted tuples", 1, ltSink.collectedTuples.size());
    Assert.assertEquals("tuples were", ltSink.collectedTuples.get(0).equals(ltuple), true);

    Assert.assertEquals("number emitted tuples", 2, lteSink.collectedTuples.size());
    Assert.assertEquals("tuples were", lteSink.collectedTuples.get(0).equals(etuple), true);
    Assert.assertEquals("tuples were", lteSink.collectedTuples.get(1).equals(ltuple), true);
  }
}
