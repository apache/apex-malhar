/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.dag.TestHashSink;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.logs.LineTokenizer}<p>
 *
 */
public class LineTokenizerTest
{
  private static Logger log = LoggerFactory.getLogger(LineTokenizerTest.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeProcessing()
  {

    LineTokenizer oper = new LineTokenizer();
    TestHashSink tokenSink = new TestHashSink();

    oper.setSplitBy(",");
    oper.tokens.setSink(tokenSink);
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null, null));
    oper.beginWindow(0); //

    String input1 = "a,b,c";
    String input2 = "a";
    String input3 = "";
    int numTuples = 1000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow(); //
    Assert.assertEquals("number emitted tuples", 3, tokenSink.map.size());
    //Assert.assertEquals("number of \"a\"", numTuples * 2, tokenSink.getCount("a"));
    //Assert.assertEquals("number of \"b\"", numTuples, tokenSink.getCount("b"));
    //Assert.assertEquals("number of \"c\"", numTuples, tokenSink.getCount("c"));
  }
}
