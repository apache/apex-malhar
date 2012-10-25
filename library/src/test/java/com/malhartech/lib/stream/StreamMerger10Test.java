/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestCountSink;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.testbench.StreamMerger10}<p>
 * Benchmarks: Currently does about 3 Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class StreamMerger10Test {

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

      oper.setup(new OperatorConfiguration());
      oper.beginWindow();

      int numtuples = 500;
      Integer input = new Integer(0);
      // Same input object can be used as the oper is just pass through
      for (int i = 0; i < numtuples; i++) {
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
      log.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", mergeSink.count));
    }
}
