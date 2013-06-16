/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.CountMap;
import com.malhartech.engine.TestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.CountMap}<p>
 *
 */
public class CountMapTest
{
  private static Logger LOG = LoggerFactory.getLogger(CountMapTest.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeProcessing()
  {
    CountMap<String, Double> oper = new CountMap<String, Double>();
    TestSink countSink = new TestSink();
    oper.count.setSink(countSink);

    oper.beginWindow(0); //

    HashMap<String, Double> input = new HashMap<String, Double>();

    input.put("a", 2.0);
    input.put("b", 20.0);
    input.put("c", 1000.0);
    oper.data.process(input);
    input.clear();
    input.put("a", 1.0);
    oper.data.process(input);
    input.clear();
    input.put("a", 10.0);
    input.put("b", 5.0);
    oper.data.process(input);
    input.clear();
    input.put("d", 55.0);
    input.put("b", 12.0);
    oper.data.process(input);
    input.clear();
    input.put("d", 22.0);
    oper.data.process(input);
    input.clear();
    input.put("d", 14.2);
    oper.data.process(input);
    input.clear();

    // Mix integers and doubles
    HashMap<String, Double> inputi = new HashMap<String, Double>();
    inputi.put("d", 46.0);
    inputi.put("e", 2.0);
    oper.data.process(inputi);
    inputi.clear();
    inputi.put("a", 23.0);
    inputi.put("d", 4.0);
    oper.data.process(inputi);
    inputi.clear();

    oper.endWindow(); //

    // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
    Assert.assertEquals("number emitted tuples", 1, countSink.collectedTuples.size());
    for (Object o: countSink.collectedTuples) {
      HashMap<String, Integer> output = (HashMap<String, Integer>)o;
      for (Map.Entry<String, Integer> e: output.entrySet()) {
        Integer val = (Integer)e.getValue();
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
