/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.MostFrequentKeyMap;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.LeastFrequentKeyInMap}<p>
 *
 */
public class MostFrequentKeyInMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MostFrequentKeyInMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    MostFrequentKeyMap<String, Integer> oper = new MostFrequentKeyMap<String, Integer>();
    CountAndLastTupleTestSink matchSink = new CountAndLastTupleTestSink();
    CountAndLastTupleTestSink listSink = new CountAndLastTupleTestSink();
    oper.most.setSink(matchSink);
    oper.list.setSink(listSink);

    oper.beginWindow(0);
    HashMap<String, Integer> amap = new HashMap<String, Integer>(1);
    HashMap<String, Integer> bmap = new HashMap<String, Integer>(1);
    HashMap<String, Integer> cmap = new HashMap<String, Integer>(1);
    int numTuples = 10000000;
    int atot = 5 * numTuples;
    int btot = 6 * numTuples;
    int ctot = 3 * numTuples;
    amap.put("a", null);
    bmap.put("b", null);
    cmap.put("c", null);
    for (int i = 0; i < atot; i++) {
      oper.data.process(amap);
    }
    for (int i = 0; i < btot; i++) {
      oper.data.process(bmap);
    }
    for (int i = 0; i < ctot; i++) {
      oper.data.process(cmap);
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 1, matchSink.count);
    HashMap<String, Integer> tuple = (HashMap<String, Integer>)matchSink.tuple;
    Integer val = tuple.get("b");
    Assert.assertEquals("Count of b was ", btot, val.intValue());
    Assert.assertEquals("number emitted tuples", 1, listSink.count);
    ArrayList<HashMap<String, Integer>> list = (ArrayList<HashMap<String, Integer>>)listSink.tuple;
    val = list.get(0).get("b");
    Assert.assertEquals("Count of b was ", btot, val.intValue());
    log.debug(String.format("\nBenchmarked %d tuples", atot + btot + ctot));
  }
}
