/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.AverageMap;
import com.datatorrent.lib.math.SumCountMap;
import com.malhartech.engine.TestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.AverageMap}. <p>
 *
 */
public class AverageMapTest
{
  private static Logger LOG = LoggerFactory.getLogger(SumCountMap.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeProcessing()
  {
    AverageMap<String, Double> oper = new AverageMap<String, Double>();
    oper.setType(Double.class);

    TestSink averageSink = new TestSink();
    oper.average.setSink(averageSink);

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

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, averageSink.collectedTuples.size());
    for (Object o: averageSink.collectedTuples) {
      HashMap<String, Object> output = (HashMap<String, Object>)o;
      for (Map.Entry<String, Object> e: output.entrySet()) {
        Double val = (Double)e.getValue();
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
  }
}
