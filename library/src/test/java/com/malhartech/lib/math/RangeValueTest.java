/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.RangeValue}<p>
 *
 */
public class RangeValueTest
{
  private static Logger LOG = LoggerFactory.getLogger(Sum.class);

  class TestSink implements Sink
  {
    List<Object> collectedTuples = new ArrayList<Object>();

    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        collectedTuples.add(payload);
      }
    }
  }

  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeSchemaProcessing()
  {
    RangeValue<Double> oper = new RangeValue<Double>();
    TestSink rangeSink = new TestSink();
    oper.range.setSink(rangeSink);

    // Not needed, but still setup is being called as a matter of discipline
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null));
    oper.beginWindow(); //

    int numTuples = 1000;
    for (int i = 0; i < numTuples; i++) {
      Double a = new Double(20.0);
      Double b = new Double(2.0);
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
    }

    oper.endWindow(); //

    // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
    Assert.assertEquals("number emitted tuples", 1, rangeSink.collectedTuples.size());
    for (Object o: rangeSink.collectedTuples) { // sum is 1157
      ArrayList<Double> list = (ArrayList<Double>)o;
      Assert.assertEquals("emitted high value was ", new Double(1000.0), list.get(0));
      Assert.assertEquals("emitted low value was ", new Double(1.0), list.get(1));
    }
  }
}
