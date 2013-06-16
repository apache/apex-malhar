/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.logs;

import com.datatorrent.lib.logs.LineTokenizerKeyVal;
import com.datatorrent.lib.testbench.HashTestSink;

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
 * Performance tests for {@link com.datatorrent.lib.logs.LineTokenizerKeyVal}<p>
 *
 */
public class LineTokenizerKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(LineTokenizerKeyValBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {

    LineTokenizerKeyVal oper = new LineTokenizerKeyVal();
    HashTestSink tokenSink = new HashTestSink();

    oper.setSplitBy(",");
    oper.setSplitTokenBy("=");
    oper.tokens.setSink(tokenSink);
    oper.beginWindow(0); //

    String input1 = "a=2,b=3,c=4";
    String input2 = "d=2";
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
