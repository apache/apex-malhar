/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.RoundRobinHashMap;
import com.datatorrent.lib.testbench.CountTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.datatorrent.lib.testbench.RoundRobinHashMap}<p>
 */
public class RoundRobinHashMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(RoundRobinHashMapBenchmark.class);

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    RoundRobinHashMap oper = new RoundRobinHashMap();
    CountTestSink mapSink = new CountTestSink();

    String[] keys = new String[3];
    keys[0] = "a";
    keys[1] = "b";
    keys[2] = "c";

    oper.setKeys(keys);
    oper.map.setSink(mapSink);
    oper.beginWindow(0);

    int numtuples = 100000000;
    for (int i = 0; i < numtuples; i++) {
      oper.data.process(i);
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", numtuples / 3, mapSink.getCount());
    log.debug(String.format("\nprocessed %d tuples", numtuples));
  }
}
