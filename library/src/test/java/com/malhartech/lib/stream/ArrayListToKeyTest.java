/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stream.StramTestSupport;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.testbench.StreamDuplicater}<p>
 * Benchmarks: Currently does about ?? Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class ArrayListToKeyTest {

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
      ArrayListToItem node = new ArrayListToItem();
      TestSink itemSink = new TestSink();
      Sink inSink = node.data.getSink();
      node.item.setSink(itemSink);
      node.setup(new OperatorConfiguration());

      Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
      inSink.process(bt);

      ArrayList<String> input = new ArrayList<String>();
      input.add("a");
      // Same input object can be used as the node is just pass through
      int numtuples = 50000000;
      for (int i = 0; i < numtuples; i++) {
        inSink.process(input);
      }
      Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
      inSink.process(et);

      // Should get one bag of keys "a", "b", "c"
      try {
        for (int i = 0; i < 100; i++) {
          Thread.sleep(10);
          if (itemSink.count >= numtuples*2 - 1) {
            break;
          }
        }
      }
      catch (InterruptedException ex) {
        LOG.debug(ex.getLocalizedMessage());
      }

      // just sleep some more, for all tuples to flush out
      for (int i = 0; i < 200; i++) {
        try {
          Thread.sleep(1);
        }
        catch (InterruptedException e) {
          LOG.error("Unexpected error while sleeping for 1 s", e);
        }
      }
      Assert.assertEquals("number emitted tuples", numtuples, itemSink.count);
      LOG.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", itemSink.count));
    }
}
