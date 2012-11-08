/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.SumValue}<p>
 *
 */
public class SumValueTest {
    private static Logger log = LoggerFactory.getLogger(SumValueTest.class);

    class TestSink implements Sink {

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
    public void testNodeProcessing() {
      testNodeSchemaProcessing(true, false);
      testNodeSchemaProcessing(true, true);
      testNodeSchemaProcessing(false, true);
    }

  public void testNodeSchemaProcessing(boolean sum, boolean count)
  {

    SumValue<Double> oper = new SumValue<Double>();
    oper.setType(Double.class);
    TestSink sumSink = new TestSink();
    TestSink countSink = new TestSink();
    if (sum) {
      oper.sum.setSink(sumSink);
    }
    if (count) {
      oper.count.setSink(countSink);
    }

    // Not needed, but still setup is being called as a matter of discipline
    oper.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));
    oper.beginWindow(0); //

    Double a = new Double(2.0);
    Double b = new Double(20.0);
    Double c = new Double(1000.0);

    oper.data.process(a);
    oper.data.process(b);
    oper.data.process(c);

    a = 1.0; oper.data.process(a);
    a = 10.0; oper.data.process(a);
    b = 5.0; oper.data.process(b);

    b = 12.0; oper.data.process(b);
    c = 22.0; oper.data.process(c);
    c = 14.0; oper.data.process(c);

    a = 46.0; oper.data.process(a);
    b = 2.0; oper.data.process(b);
    a = 23.0; oper.data.process(a);

    oper.endWindow(); //

    if (sum) {
      // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
      Assert.assertEquals("number emitted tuples", 1, sumSink.collectedTuples.size());
       for (Object o: sumSink.collectedTuples) { // sum is 1157
        Double val = (Double)o;
        Assert.assertEquals("emitted sum value was was ", new Double(1157.0), o);
      }
    }
    if (count) {
      // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
      Assert.assertEquals("number emitted tuples", 1, countSink.collectedTuples.size());
      for (Object o: countSink.collectedTuples) { // count is 12
        Integer val = (Integer) o;
        Assert.assertEquals("emitted sum value was was ", new Integer(12), val);
      }
    }
  }
}
