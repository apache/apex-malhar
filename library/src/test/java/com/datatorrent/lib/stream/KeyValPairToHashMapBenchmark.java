/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.KeyValPairToHashMap;
import com.datatorrent.lib.testbench.CountTestSink;
import com.datatorrent.lib.util.KeyValPair;

import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.datatorrent.lib.testbench.KeyValPairToHashMap}<p>
 * <br>
 */
public class KeyValPairToHashMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(KeyValPairToHashMapBenchmark.class);

  /**
   * Test oper pass through. The Object passed is not relevant
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    KeyValPairToHashMap oper = new KeyValPairToHashMap();
    CountTestSink mapSink = new CountTestSink();

    oper.map.setSink(mapSink);

    oper.beginWindow(0);
    KeyValPair<String, String> input = new KeyValPair<String, String>("a", "1");

    // Same input object can be used as the oper is just pass through
    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
      oper.keyval.process(input);
    }

    oper.endWindow();
    log.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", numTuples));
  }
}
