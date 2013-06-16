/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.AllAfterMatchMap;
import com.malhartech.engine.TestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.AllAfter} <p>
 */
public class AllAfterMatchMapTest
{
  private static Logger log = LoggerFactory.getLogger(AllAfterMatchMapTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new AllAfterMatchMap<String, Integer>());
    testNodeProcessingSchema(new AllAfterMatchMap<String, Double>());
    testNodeProcessingSchema(new AllAfterMatchMap<String, Float>());
    testNodeProcessingSchema(new AllAfterMatchMap<String, Short>());
    testNodeProcessingSchema(new AllAfterMatchMap<String, Long>());
  }

  public void testNodeProcessingSchema(AllAfterMatchMap oper)
  {
    TestSink allSink = new TestSink();
    oper.allafter.setSink(allSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 2);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 3);
    oper.data.process(input);


    input.clear();
    input.put("b", 6);
    oper.data.process(input);

    input.clear();
    input.put("c", 9);
    oper.data.process(input);

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3, allSink.collectedTuples.size());
    for (Object o: allSink.collectedTuples) {
      for (Map.Entry<String, Number> e: ((HashMap<String, Number>)o).entrySet()) {
        if (e.getKey().equals("a")) {
          Assert.assertEquals("emitted value for 'a' was ", new Double(3), e.getValue().doubleValue());
        }
        else if (e.getKey().equals("b")) {
          Assert.assertEquals("emitted tuple for 'b' was ", new Double(6), e.getValue().doubleValue());
        }
        else if (e.getKey().equals("c")) {
          Assert.assertEquals("emitted tuple for 'c' was ", new Double(9), e.getValue().doubleValue());
        }
      }
    }
  }
}
