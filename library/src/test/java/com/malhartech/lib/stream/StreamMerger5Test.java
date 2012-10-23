/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.OperatorContext;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stream.StramTestSupport;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.testbench.StreamMerger5}<p>
 * Benchmarks: Currently does about 3 Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class StreamMerger5Test {

    private static Logger LOG = LoggerFactory.getLogger(StreamMerger.class);

    class TestSink implements Sink {
        int count = 0;

        /**
         *
         * @param payload
         */
        @Override
        public void process(Object payload) {
          if (payload instanceof Tuple) {
          }
          else {
            count++;
          }
        }
    }

    /**
     * Test node pass through. The Object passed is not relevant
     */
    @Test
    public void testNodeProcessing() throws Exception {

      StreamMerger5 node = new StreamMerger5();
      TestSink mergeSink = new TestSink();

      Sink inSink1 = node.data1.getSink();
      Sink inSink2 = node.data2.getSink();
      Sink inSink3 = node.data3.getSink();
      Sink inSink4 = node.data4.getSink();
      Sink inSink5 = node.data5.getSink();
      node.out.setSink(mergeSink);
      node.setup(new OperatorConfiguration());

      Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
      inSink1.process(bt);
      inSink2.process(bt);
      inSink3.process(bt);
      inSink4.process(bt);
      inSink5.process(bt);

      int numtuples = 50000000;
      Integer input = new Integer(0);
      // Same input object can be used as the node is just pass through
      for (int i = 0; i < numtuples; i++) {
        inSink1.process(input);
        inSink2.process(input);
        inSink3.process(input);
        inSink4.process(input);
        inSink5.process(input);
      }

      Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
      inSink1.process(et);
      inSink2.process(et);
      inSink3.process(et);
      inSink4.process(et);
      inSink5.process(et);

      // Should get one bag of keys "a", "b", "c"
      try {
        for (int i = 0; i < 900; i++) {
          Thread.sleep(10);
          if (mergeSink.count >= numtuples*2 - 1) {
            break;
          }
        }
      }
      catch (InterruptedException ex) {
        LOG.debug(ex.getLocalizedMessage());
      }

      LOG.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", mergeSink.count));
    }
}
