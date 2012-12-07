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
 * Performance test for {@link com.malhartech.lib.testbench.StreamMerger10}<p>
 * Benchmarks: Currently does about 3 Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class StreamMerger10Test
{
  private static Logger log = LoggerFactory.getLogger(StreamMerger10Test.class);

  /**
   * Test oper pass through. The Object passed is not relevant
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    final StreamMerger10 oper = new StreamMerger10();
    TestCountSink mergeSink = new TestCountSink();

    oper.out.setSink(mergeSink);

    oper.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));
    oper.beginWindow(0);

    int numTuples = 1000;
    Integer input = new Integer(0);
    // Same input object can be used as the oper is just pass through
    for (int i = 0; i < numTuples; i++) {
      oper.data1.process(input);
      oper.data2.process(input);
      oper.data3.process(input);
      oper.data4.process(input);
      oper.data5.process(input);
      oper.data6.process(input);
      oper.data7.process(input);
      oper.data8.process(input);
      oper.data9.process(input);
      oper.data10.process(input);
    }

    oper.endWindow();
    Assert.assertEquals("number emitted tuples", numTuples * 10, mergeSink.count);
  }
}
