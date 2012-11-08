/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.AllAfterMatchStringValue}. <p>
 *
 */
public class AllAfterMatchStringValueTest
{
  private static Logger log = LoggerFactory.getLogger(AllAfterMatchStringValueTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {

    AllAfterMatchStringValue<String> oper = new AllAfterMatchStringValue<String>();
    TestSink<HashMap<String, String>> allSink = new TestSink<HashMap<String, String>>();
    oper.allafter.setSink(allSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, String> input = new HashMap<String, String>();
    input.put("a", "2");
    input.put("b", "20");
    input.put("c", "1000");
    oper.data.process(input);
    input.clear();
    input.put("a", "3");
    oper.data.process(input);

    input.clear();
    input.put("b", "6");
    oper.data.process(input);

    input.clear();
    input.put("c", "9");
    oper.data.process(input);

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3, allSink.collectedTuples.size());
    for (Object o: allSink.collectedTuples) {
      for (Map.Entry<String, String> e: ((HashMap<String, String>)o).entrySet()) {
        if (e.getKey().equals("a")) {
          Assert.assertEquals("emitted value for 'a' was ", "3", e.getValue());
        }
        else if (e.getKey().equals("b")) {
          Assert.assertEquals("emitted tuple for 'b' was ", "6", e.getValue());
        }
        else if (e.getKey().equals("c")) {
          Assert.assertEquals("emitted tuple for 'c' was ", "9", e.getValue());
        }
      }
    }
  }
}
