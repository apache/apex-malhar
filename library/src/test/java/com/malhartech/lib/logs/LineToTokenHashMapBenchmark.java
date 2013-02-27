/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.lib.testbench.TestHashSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.logs.LineToTokenHashMap}<p>
 *
 */
public class LineToTokenHashMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(LineToTokenHashMapBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {

    LineToTokenHashMap oper = new LineToTokenHashMap();
    TestHashSink tokenSink = new TestHashSink();

    oper.setSplitBy(";");
    oper.setSplitTokenBy(",");
    oper.tokens.setSink(tokenSink);

    oper.beginWindow(0); //
    String input1 = "a,2,3;b,1,2;c,4,5,6";
    String input2 = "d";
    String input3 = "";
    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow(); //
    Assert.assertEquals("number emitted tuples", 2, tokenSink.map.size());
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 3));
  }
}
