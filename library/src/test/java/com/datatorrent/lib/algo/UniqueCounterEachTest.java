/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.algo.UniqueCounterEach;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.UniqueCounterEach}<p>
 *
 */
public class UniqueCounterEachTest
{
  private static Logger log = LoggerFactory.getLogger(UniqueCounterEachTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    UniqueCounterEach<String> oper = new UniqueCounterEach<String>();
    TestSink sink = new TestSink();
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
    int acount = 0;
    int bcount = 0;
    int ccount = 0;
    int dcount = 0;
    int ecount = 0;
    for (Object ee: sink.collectedTuples) {
      HashMap<String, Integer> e = (HashMap<String, Integer>)ee;
      Integer val = e.get("a");
      if (val != null) {
        acount = val.intValue();
      }
      val = e.get("b");
      if (val != null) {
        bcount = val.intValue();
      }
      val = e.get("c");
      if (val != null) {
        ccount = val.intValue();
      }
      val = e.get("d");
      if (val != null) {
        dcount = val.intValue();
      }
      val = e.get("e");
      if (val != null) {
        ecount = val.intValue();
      }
    }
    Assert.assertEquals("number emitted tuples", 5, sink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", numTuples, acount);
    Assert.assertEquals("number emitted tuples", numTuples / 2, bcount);
    Assert.assertEquals("number emitted tuples", numTuples / 3 + 1, ccount);
    Assert.assertEquals("number emitted tuples", numTuples / 5, dcount);
    Assert.assertEquals("number emitted tuples", numTuples / 10, ecount);
  }
}
