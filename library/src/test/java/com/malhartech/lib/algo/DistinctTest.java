/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestHashSink;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.Distinct<p>
 *
 */
public class DistinctTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    Distinct<String> oper = new Distinct<String>();

    TestHashSink<String> sortSink = new TestHashSink<String>();
    oper.distinct.setSink(sortSink);

    oper.beginWindow(0);
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("b");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("b");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("c");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("c");
    oper.data.process("d");
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 4, sortSink.size());
    Assert.assertEquals("number of \"a\"", 1, sortSink.getCount("a"));
    Assert.assertEquals("number of \"b\"", 1, sortSink.getCount("b"));
    Assert.assertEquals("number of \"c\"", 1, sortSink.getCount("c"));
    Assert.assertEquals("number of \"d\"", 1, sortSink.getCount("d"));
  }
}
