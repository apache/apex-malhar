/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.ArrayListAggregator;
import com.malhartech.engine.TestSink;
import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.datatorrent.lib.testbench.ArrayListAggregator}<p>
 */
public class ArrayListAggregatorTest
{
  private static Logger log = LoggerFactory.getLogger(ArrayListAggregatorTest.class);

  @Test
  public void testNodeProcessing() throws Exception
  {
    ArrayListAggregator<Integer> oper = new ArrayListAggregator<Integer>();
    TestSink cSink = new TestSink();

    oper.output.setSink(cSink);
    oper.setSize(10);
    int numtuples = 100;

    oper.beginWindow(0);
    for (int i = 0; i < numtuples; i++) {
      oper.input.process(i);
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 10, cSink.collectedTuples.size());

    cSink.clear();
    oper.setSize(0);

    oper.beginWindow(1);
    for (int i = 0; i < numtuples; i++) {
      oper.input.process(i);
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 1, cSink.collectedTuples.size());
    ArrayList list = (ArrayList) cSink.collectedTuples.get(0);
    Assert.assertEquals("number emitted tuples", numtuples, list.size());
  }
}
