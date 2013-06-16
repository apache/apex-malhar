/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.StreamDuplicater;
import com.datatorrent.lib.testbench.CountTestSink;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.datatorrent.lib.testbench.StreamDuplicater}<p>
 * Benchmarks: Currently does about ?? Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class StreamDuplicaterBenchmark
{
  private static Logger log = LoggerFactory.getLogger(StreamDuplicaterBenchmark.class);

  /**
   * Test oper pass through. The Object passed is not relevant
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    StreamDuplicater oper = new StreamDuplicater();
    CountTestSink mergeSink1 = new CountTestSink();
    CountTestSink mergeSink2 = new CountTestSink();

    oper.out1.setSink(mergeSink1);
    oper.out2.setSink(mergeSink2);

    oper.beginWindow(0);
    int numTuples = 200000000;
    Integer input = new Integer(0);
    // Same input object can be used as the oper is just pass through
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input);
    }

    oper.endWindow();
    log.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", numTuples));
  }
}
