/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.StreamMerger5;
import com.datatorrent.lib.testbench.CountTestSink;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.datatorrent.lib.testbench.StreamMerger5}<p>
 * Benchmarks: Currently does about 3 Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class StreamMerger5Benchmark
{
  private static Logger log = LoggerFactory.getLogger(StreamMerger5Benchmark.class);

  /**
   * Test oper pass through. The Object passed is not relevant
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {

    StreamMerger5 oper = new StreamMerger5();
    CountTestSink mergeSink = new CountTestSink();
    oper.out.setSink(mergeSink);

    oper.beginWindow(0);
    int numTuples = 200000000;
    Integer input = new Integer(0);
    // Same input object can be used as the oper is just pass through
    for (int i = 0; i < numTuples; i++) {
      oper.data1.process(input);
      oper.data2.process(input);
      oper.data3.process(input);
      oper.data4.process(input);
      oper.data5.process(input);
    }

    oper.endWindow();
    log.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", numTuples*5));
  }
}
