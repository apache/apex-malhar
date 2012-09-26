/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.dag.Component;
import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.ModuleContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stram.ManualScheduledExecutorService;
import com.malhartech.stram.WindowGenerator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests {@link com.malhartech.lib.testbench.LoadGenerator} at a very high load with stringschema. Current peak benchmark is at 16 Million tuples/sec<p>
 * <br>
 * The benchmark results matter a lot in terms of how thread contention is handled. The test has three parts<br>
 * 1. Trigger the input generator with a very high load and no wait. Simultaneously start read thread, but do sleep
 * once a while. Set the buffersize large enough to handle growing queued up tuples<br>
 * 2. Deactivate load generator node and drain the queue<br>
 * 3. Wait till all the queue is drained<br>
 * <br>
 * No DRC check is done on the node as this test is for benchmark only<br>
 * <br>
 * Benchmark is at 16 Million tuples/src. Once we get to real Hadoop cluster, we should increase the buffer size to handle 100x more tuples and see what the raw
 * throughput would be. Then on we would not need to force either thread to wait, or be hampered by low memory on debugging
 * envinronment<br>
 * <br>
 */
public class BenchmarkLoadGenerator {

    private static Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);

    class TestSink implements Sink {
        HashMap<String, Integer> collectedTuples = new HashMap<String, Integer>();

        //DefaultSerDe serde = new DefaultSerDe();
        int count = 0;
        boolean dohash = false;
        /**
         *
         * @param payload
         */
        @Override
      public void process(Object payload)
      {
        count++; // Behchmark counts all tuples as we are measuring throughput
        if (dohash) {
          if (payload instanceof Tuple) {
            // LOG.debug(payload.toString());
          }
          else { // ignore the payload, just count it
            String str = (String)payload;
            Integer val = collectedTuples.get(str);
            if (val == null) {
              val = new Integer(1);
            }
            else {
              val = val + 1;
            }
            collectedTuples.put(str, val);
          }
        }
      }
    }

    class TestCountSink implements Sink {

        //DefaultSerDe serde = new DefaultSerDe();
        int count = 0;
        int average = 0;
        int num_tuples = 0;

        /**
         *
         * @param payload
         */
        @Override
        public void process(Object payload) {
            if (payload instanceof Tuple) {
                // LOG.debug(payload.toString());
            }
            else {
              ArrayList<Integer> iarray = (ArrayList<Integer>) payload;
              count += iarray.get(1).intValue();
              num_tuples++;
             }
        }
    }


    /**
     * Benchmark the maximum payload flow for String
     * The sink would simply ignore the payload as we are testing throughput
     */
    @Test
    @Category(com.malhartech.PerformanceTestCategory.class)
    public void testNodeProcessing() throws Exception {

        final LoadGenerator node = new LoadGenerator();
        final ManualScheduledExecutorService mses = new ManualScheduledExecutorService(1);
        final WindowGenerator wingen = new WindowGenerator(mses);

        Configuration config = new Configuration();
        config.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, 0);
        config.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 1);
        wingen.setup(config);

        Sink input = node.connect(Component.INPUT, wingen);
        wingen.connect("mytestnode", input);

        TestSink lgenSink = new TestSink();
        TestCountSink countSink = new TestCountSink();
        node.connect(LoadGenerator.OPORT_DATA, lgenSink);
        //node.connect(LoadGenerator.OPORT_COUNT, countSink);

        ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
        lgenSink.dohash = false;

        conf.set(LoadGenerator.KEY_KEYS, "a");
        conf.set(LoadGenerator.KEY_STRING_SCHEMA, "true");
        conf.setInt(LoadGenerator.KEY_TUPLES_BLAST, 50000000);
        conf.setInt(LoadGenerator.KEY_SLEEP_TIME, 25);
        conf.setInt("SpinMillis", 5);
        conf.setInt("BufferCapacity", 2 * 1024 * 1024);

      node.setup(conf);

        final AtomicBoolean inactive = new AtomicBoolean(true);
        new Thread() {
            @Override
            public void run() {
                inactive.set(false);
                node.activate(new ModuleContext("LoadGeneratorTestNode", this));
            }
        }.start();

        /**
         * spin while the node gets activated.
         */
        try {
            do {
                Thread.sleep(20);
            } while (inactive.get());
        } catch (InterruptedException ex) {
            LOG.debug(ex.getLocalizedMessage());
        }
        wingen.activate(null);
        for (int i = 0; i < 7000; i++) {
            mses.tick(1);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                LOG.error("Unexpected error while sleeping for 1 s", e);
            }
        }
        node.deactivate();

      //LOG.debug(String.format("\nProcessed %d tuples from emitted %d in %d windows", lgenSink.count, countSink.count, countSink.num_tuples));
      LOG.debug(String.format("\nProcessed %d tuples", lgenSink.count));
      for (Map.Entry<String, Integer> e: lgenSink.collectedTuples.entrySet()) {
        LOG.debug("{} tuples for key {}", e.getValue().intValue(), e.getKey());
      }
    }
}
