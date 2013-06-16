/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.algo.UniqueCounterValue;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.UniqueCounterValue}<p>
 *
 */
public class UniqueCounterValueTest
{
  private static Logger log = LoggerFactory.getLogger(UniqueCounterValueTest.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    UniqueCounterValue<String> oper = new UniqueCounterValue<String>();
    TestSink sink = new TestSink();
    oper.count.setSink(sink);

    int numTuples = 1000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process("a");
      oper.data.process("b");
      oper.data.process("c");
      oper.data.process("d");
      oper.data.process("e");
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples to TestSink", 1, sink.collectedTuples.size());
    Assert.assertEquals("count emitted tuples", numTuples*5, sink.collectedTuples.get(0));
  }
}
