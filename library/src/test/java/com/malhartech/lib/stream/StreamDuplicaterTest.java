/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stream.StramTestSupport;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.testbench.StreamDuplicater}<p>
 * Benchmarks: Currently does about ?? Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class StreamDuplicaterTest {

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
    public void testNodeProcessing() throws Exception
    {
      StreamDuplicater node = new StreamDuplicater();
      TestSink mergeSink1 = new TestSink();
      TestSink mergeSink2 = new TestSink();
      Sink inSink = node.data.getSink();
      node.out1.setSink(mergeSink1);
      node.out2.setSink(mergeSink2);
      node.setup(new OperatorConfiguration());


      Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
      inSink.process(bt);

      int numtuples = 50000000;
      Integer input = new Integer(0);
      // Same input object can be used as the node is just pass through
      for (int i = 0; i < numtuples; i++) {
        inSink.process(input);
      }
      Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
      inSink.process(et);

      // Should get one bag of keys "a", "b", "c"
      try {
        for (int i = 0; i < 100; i++) {
          Thread.sleep(10);
          if (mergeSink1.count >= numtuples*2 - 1) {
            break;
          }
        }
      }
      catch (InterruptedException ex) {
        LOG.debug(ex.getLocalizedMessage());
      }

      // One for each key
      Assert.assertEquals("number emitted tuples", numtuples, mergeSink1.count);
      Assert.assertEquals("number emitted tuples", numtuples, mergeSink2.count);
      LOG.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", mergeSink1.count+mergeSink2.count));
    }
}
