/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.engine.TestCountSink;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.testbench.StreamMerger}<p>
 * Benchmarks: Currently does about 3 Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class StreamMergerTest
{
  private static Logger log = LoggerFactory.getLogger(StreamMergerTest.class);

  /**
   * Test oper pass through. The Object passed is not relevant
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    StreamMerger oper = new StreamMerger();
    TestCountSink mergeSink = new TestCountSink();
    oper.out.setSink(mergeSink);

    oper.beginWindow(0);
    int numtuples = 500;
    Integer input = new Integer(0);
    // Same input object can be used as the oper is just pass through
    for (int i = 0; i < numtuples; i++) {
      oper.data1.process(input);
      oper.data2.process(input);
    }

    oper.endWindow();
    Assert.assertEquals("number emitted tuples", numtuples*2, mergeSink.count);
  }
}
