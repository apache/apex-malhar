/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.algo.UniqueKeyValCounterEach;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.UniqueKeyValCounterEach}<p>
 *
 */
public class UniqueKeyValCounterEachBenchmark
{
  private static Logger log = LoggerFactory.getLogger(UniqueKeyValCounterEachBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    UniqueKeyValCounterEach<String,Integer> oper = new UniqueKeyValCounterEach<String,Integer>();
    TestSink sink = new TestSink();
    oper.count.setSink(sink);

    HashMap<String,Integer> a1tuple = new HashMap<String,Integer>(1);
    HashMap<String,Integer> a2tuple = new HashMap<String,Integer>(1);
    HashMap<String,Integer> btuple = new HashMap<String,Integer>(1);
    HashMap<String,Integer> ctuple = new HashMap<String,Integer>(1);
    HashMap<String,Integer> dtuple = new HashMap<String,Integer>(1);
    HashMap<String,Integer> e1tuple = new HashMap<String,Integer>(1);
    HashMap<String,Integer> e2tuple = new HashMap<String,Integer>(1);

    a1tuple.put("a",1);
    a2tuple.put("a",2);
    btuple.put("b",4);
    ctuple.put("c",3);
    dtuple.put("d",1001);
    e1tuple.put("e",1091919);
    e2tuple.put("e",808);
    e2tuple.put("c",3);


    int numTuples = 10000000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(a1tuple);
      if (i % 2 == 0) {
        oper.data.process(btuple);
        oper.data.process(e2tuple);
      }
      if (i % 3 == 0) {
        oper.data.process(ctuple);
      }
      if (i % 5 == 0) {
        oper.data.process(dtuple);
        oper.data.process(a2tuple);
      }
      if (i % 10 == 0) {
        oper.data.process(e1tuple);
      }
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 7, sink.collectedTuples.size());

    HashMap<HashMap<String,Integer>,Integer> tuple = new HashMap<HashMap<String,Integer>,Integer>();
    for (Object o : sink.collectedTuples) {
      for (Map.Entry<HashMap<String,Integer>, Integer> e: ((HashMap<HashMap<String,Integer>,Integer>)o).entrySet()) {
        tuple.put(e.getKey(),e.getValue());
      }
    }

    int a1count = tuple.get(a1tuple).intValue();
    int a2count = tuple.get(a2tuple).intValue();
    int bcount = tuple.get(btuple).intValue();
    int ccount = tuple.get(ctuple).intValue();
    int dcount = tuple.get(dtuple).intValue();
    int e1count = tuple.get(e1tuple).intValue();
    e2tuple.clear();
    e2tuple.put("e",808);
    int e2count = tuple.get(e2tuple).intValue();
    int count = a1count + a2count + bcount + ccount + dcount + e1count + e2count;
    log.debug(String.format("\nBenchmarked %d key,val pairs", count));
  }
}
