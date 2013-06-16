/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.Average;
import com.malhartech.engine.TestSink;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.Average}. <p>
 *
 */
public class AverageTest
{
  private static Logger log = LoggerFactory.getLogger(AverageTest.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeProcessing()
  {
    Average<Double> doper = new Average<Double>();
    Average<Float> foper = new Average<Float>();
    Average<Integer> ioper = new Average<Integer>();
    Average<Long> loper = new Average<Long>();
    Average<Short> soper = new Average<Short>();
    doper.setType(Double.class);
    foper.setType(Float.class);
    ioper.setType(Integer.class);
    loper.setType(Long.class);
    soper.setType(Short.class);

    testNodeSchemaProcessing(doper);
    testNodeSchemaProcessing(foper);
    testNodeSchemaProcessing(ioper);
    testNodeSchemaProcessing(loper);
    testNodeSchemaProcessing(soper);
  }

  public void testNodeSchemaProcessing(Average oper)
  {
    TestSink averageSink = new TestSink();
    oper.average.setSink(averageSink);

    oper.beginWindow(0); //

    Double a = new Double(2.0);
    Double b = new Double(20.0);
    Double c = new Double(1000.0);

    oper.data.process(a);
    oper.data.process(b);
    oper.data.process(c);

    a = 1.0;
    oper.data.process(a);
    a = 10.0;
    oper.data.process(a);
    b = 5.0;
    oper.data.process(b);

    b = 12.0;
    oper.data.process(b);
    c = 22.0;
    oper.data.process(c);
    c = 14.0;
    oper.data.process(c);

    a = 46.0;
    oper.data.process(a);
    b = 2.0;
    oper.data.process(b);
    a = 23.0;
    oper.data.process(a);

    oper.endWindow(); //

    Assert.assertEquals("number emitted tuples", 1, averageSink.collectedTuples.size());
    for (Object o: averageSink.collectedTuples) { // count is 12
      Integer val = ((Number)o).intValue();
      Assert.assertEquals("emitted average value was was ", new Integer(1157 / 12), val);
    }
  }
}
