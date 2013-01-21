/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;
import com.malhartech.lib.util.KeyValPair;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.SumKeyVal}<p>
 *
 */
public class SumKeyValTest
{
  private static Logger LOG = LoggerFactory.getLogger(MapSum.class);

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
   * Test operator logic emits correct results
   */
  @Test
  public void testNodeProcessing()
  {
    testNodeSchemaProcessing(true, false,  false);
    testNodeSchemaProcessing(false, true, false);
    testNodeSchemaProcessing(false, false, true);
    testNodeSchemaProcessing(true, true, false);
    testNodeSchemaProcessing(false, true, true);
    testNodeSchemaProcessing(true, false, true);
  }

  public void testNodeSchemaProcessing(boolean sum, boolean count, boolean average)
  {
    SumKeyVal<String, Double> oper = new SumKeyVal<String, Double>();
    oper.setType(Double.class);
    TestSink sumSink = new TestSink();
    TestSink countSink = new TestSink();
    TestSink averageSink = new TestSink();
    if (sum) {
      oper.sum.setSink(sumSink);
    }
    if (average){
      oper.average.setSink(averageSink);
    }
    if (count) {
      oper.count.setSink(countSink);
    }

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
    if (sum) {
      Assert.assertEquals("number emitted tuples", 5, sumSink.collectedTuples.size());
      for (Object o: sumSink.collectedTuples) {
        KeyValPair<String, Double> e = (KeyValPair<String, Double>)o;
        Double val = (Double)e.getValue();
        if (e.getKey().equals("a")) {
          Assert.assertEquals("emitted value for 'a' was ", new Double(36), val);
        }
        else if (e.getKey().equals("b")) {
          Assert.assertEquals("emitted tuple for 'b' was ", new Double(37), val);
        }
        else if (e.getKey().equals("c")) {
          Assert.assertEquals("emitted tuple for 'c' was ", new Double(1000), val);
        }
        else if (e.getKey().equals("d")) {
          Assert.assertEquals("emitted tuple for 'd' was ", new Double(141.2), val);
        }
        else if (e.getKey().equals("e")) {
          Assert.assertEquals("emitted tuple for 'e' was ", new Double(2), val);
        }
      }
    }
    if (average) {
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

    if (count) {
      Assert.assertEquals("number emitted tuples", 5, countSink.collectedTuples.size());
      for (Object o: countSink.collectedTuples) {
        KeyValPair<String, Integer> e = (KeyValPair<String, Integer>) o;
        Integer val = e.getValue();
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
}
