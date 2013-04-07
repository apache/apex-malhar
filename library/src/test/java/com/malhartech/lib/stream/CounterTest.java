/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.engine.TestSink;
import com.malhartech.lib.testbench.CountTestSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.malhartech.lib.testbench.Counter}<p>
 * <br>
 */
public class CounterTest {

    private static Logger log = LoggerFactory.getLogger(CounterTest.class);


    /**
     * Test oper pass through. The Object passed is not relevant
     */
    @Test
    public void testNodeProcessing() throws Exception
    {
      Counter oper = new Counter();
      CountTestSink cSink = new CountTestSink();

      oper.output.setSink(cSink);
      int numtuples = 100;

      oper.beginWindow(0);
      for (int i = 0; i < numtuples; i++) {
        oper.input.process(i);
      }
      oper.endWindow();

      oper.beginWindow(1);
      for (int i = 0; i < numtuples; i++) {
        oper.input.process(i);
      }
      oper.endWindow();

      Assert.assertEquals("number emitted tuples", 2, cSink.getCount());
    }
}
