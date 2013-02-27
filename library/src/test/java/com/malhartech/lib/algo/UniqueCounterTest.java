/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.testbench.TestCountAndLastTupleSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.UniqueCounter}<p>
 *
 */
public class UniqueCounterTest
{
  private static Logger log = LoggerFactory.getLogger(UniqueCounterTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    UniqueCounter<String> oper = new UniqueCounter<String>();
    TestCountAndLastTupleSink<HashMap<String, Integer>> sink = new TestCountAndLastTupleSink<HashMap<String, Integer>>();
    oper.count.setSink(sink);

    String atuple = "a";
    String btuple = "b";
    String ctuple = "c";
    String dtuple = "d";
    String etuple = "e";

    int numTuples = 10000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(atuple);
      if (i % 2 == 0) {
        oper.data.process(btuple);
      }
      if (i % 3 == 0) {
        oper.data.process(ctuple);
      }
      if (i % 5 == 0) {
        oper.data.process(dtuple);
      }
      if (i % 10 == 0) {
        oper.data.process(etuple);
      }
    }
    oper.endWindow();
    HashMap<String,Integer> tuple = (HashMap<String,Integer>) sink.tuple;
    int acount = tuple.get("a").intValue();
    int bcount = tuple.get("b").intValue();
    int ccount = tuple.get("c").intValue();
    int dcount = tuple.get("d").intValue();
    int ecount = tuple.get("e").intValue();
    Assert.assertEquals("number emitted tuples", 1, sink.count);
    Assert.assertEquals("number emitted tuples", numTuples, acount);
    Assert.assertEquals("number emitted tuples", numTuples/2, bcount);
    Assert.assertEquals("number emitted tuples", numTuples/3 + 1, ccount);
    Assert.assertEquals("number emitted tuples", numTuples/5, dcount);
    Assert.assertEquals("number emitted tuples", numTuples/10, ecount);
  }
}
