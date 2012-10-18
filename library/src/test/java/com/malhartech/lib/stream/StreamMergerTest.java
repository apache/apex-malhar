/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.dag.OperatorConfiguration;
import com.malhartech.dag.OperatorContext;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stream.StramTestSupport;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.testbench.StreamMerger}<p>
 * Benchmarks: Currently does about 3 Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class StreamMergerTest {

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

      final StreamMerger node = new StreamMerger();

      TestSink mergeSink = new TestSink();

      Sink inSink1 = node.connect(StreamMerger.IPORT_IN_DATA1, node);
      Sink inSink2 = node.connect(StreamMerger.IPORT_IN_DATA2, node);
      node.connect(StreamMerger.OPORT_OUT_DATA, mergeSink);

      OperatorConfiguration conf = new OperatorConfiguration("mynode", new HashMap<String, String>());
      conf.setInt("SpinMillis", 10);
      conf.setInt("BufferCapacity", 1024 * 1024);
      node.setup(conf);

      final AtomicBoolean inactive = new AtomicBoolean(true);
      new Thread()
      {
        @Override
        public void run()
        {
          inactive.set(false);
          node.activate(new OperatorContext("StreamMergerTestNode", this));
        }
      }.start();

      // spin while the node gets activated./
      int sleeptimes = 0;
      try {
        do {
          Thread.sleep(20);
          sleeptimes++;
          if (sleeptimes > 5) {
            break;
          }
        }
        while (inactive.get());
      }
      catch (InterruptedException ex) {
        LOG.debug(ex.getLocalizedMessage());
      }

      Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
      inSink1.process(bt);
      inSink2.process(bt);

      int numtuples = 50000000;
      Integer input = new Integer(0);
      // Same input object can be used as the node is just pass through
      for (int i = 0; i < numtuples; i++) {
        inSink1.process(input);
        inSink2.process(input);
      }

      Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
      inSink1.process(et);
      inSink2.process(et);

      // Should get one bag of keys "a", "b", "c"
      try {
        for (int i = 0; i < 100; i++) {
          Thread.sleep(10);
          if (mergeSink.count >= numtuples*2 - 1) {
            break;
          }
        }
      }
      catch (InterruptedException ex) {
        LOG.debug(ex.getLocalizedMessage());
      }

      // One for each key
      Assert.assertEquals("number emitted tuples", numtuples*2, mergeSink.count);
      LOG.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", mergeSink.count));
      for (int i = 1; i <= node.getNumInputPorts(); i++) {
        LOG.debug(String.format("%dth input port name is %s", i, node.getInputName(i)));
      }
    }
}
