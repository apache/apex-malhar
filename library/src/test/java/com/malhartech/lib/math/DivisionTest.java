/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestSink;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.Division}<p>
 *
 */
public class DivisionTest
{
  private static Logger log = LoggerFactory.getLogger(DivisionTest.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeProcessing()
  {
    Division oper = new Division();
    TestSink lqSink = new TestSink();
    TestSink iqSink = new TestSink();
    TestSink dqSink = new TestSink();
    TestSink fqSink = new TestSink();
    TestSink lrSink = new TestSink();
    TestSink irSink = new TestSink();
    TestSink drSink = new TestSink();
    TestSink frSink = new TestSink();
    TestSink eSink = new TestSink();

    oper.longQuotient.setSink(lqSink);
    oper.integerQuotient.setSink(iqSink);
    oper.doubleQuotient.setSink(dqSink);
    oper.floatQuotient.setSink(fqSink);
    oper.longRemainder.setSink(lrSink);
    oper.doubleRemainder.setSink(drSink);
    oper.floatRemainder.setSink(frSink);
    oper.integerRemainder.setSink(irSink);
    oper.errordata.setSink(eSink);

    oper.beginWindow(0); //
    oper.denominator.process(5);
    oper.numerator.process(11);
    oper.denominator.process(0);
    oper.endWindow(); //

    Assert.assertEquals("number emitted tuples", 1, lqSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1, iqSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1, dqSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1, fqSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1, lrSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1, irSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1, drSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1, frSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1, eSink.collectedTuples.size());

    Assert.assertEquals("quotient is", new Long(2), lqSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", 2, iqSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", 2.2, dqSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", new Float(2.2), fqSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", new Long(1), lrSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", 1, irSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", 1.0, drSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", new Float(1.0), frSink.collectedTuples.get(0));
  }
}
