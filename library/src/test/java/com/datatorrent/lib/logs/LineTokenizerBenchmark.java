/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.logs;

import com.datatorrent.lib.logs.LineTokenizer;
import com.datatorrent.lib.math.*;
import com.datatorrent.lib.testbench.HashTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.logs.LineTokenizer}<p>
 *
 */
public class LineTokenizerBenchmark
{
  private static Logger log = LoggerFactory.getLogger(LineTokenizerBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {

    LineTokenizer oper = new LineTokenizer();
    HashTestSink tokenSink = new HashTestSink();

    oper.setSplitBy(",");
    oper.tokens.setSink(tokenSink);
    oper.beginWindow(0); //

    String input1 = "a,b,c";
    String input2 = "a";
    String input3 = "";
    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow(); //
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 3));
    //Assert.assertEquals("number of \"a\"", numTuples * 2, tokenSink.getCount("a"));
    //Assert.assertEquals("number of \"b\"", numTuples, tokenSink.getCount("b"));
    //Assert.assertEquals("number of \"c\"", numTuples, tokenSink.getCount("c"));
  }
}
