/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.dag.Component;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stram.ManualScheduledExecutorService;
import com.malhartech.stram.WindowGenerator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
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
        public void process(Object payload) {
            if (payload instanceof Tuple) {
                // LOG.debug(payload.toString());
            }
          else { // ignore the payload, just count it
            count++;
            if (dohash) {
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


    /**
     * Benchmark the maximum payload flow for String
     * The sink would simply ignore the payload as we are testing througput
     */
    @Test
    public void testNodeProcessing() {

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
        node.connect(LoadGenerator.OPORT_DATA, lgenSink);
        NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());
        lgenSink.dohash = false;

        conf.set(LoadGenerator.KEY_KEYS, "a");
        conf.set(LoadGenerator.KEY_VALUES, "");
        conf.set(LoadGenerator.KEY_STRING_SCHEMA, "true");
        conf.setInt(LoadGenerator.KEY_TUPLES_PER_SEC, 1000000000);
        conf.setInt("SpinMillis", 10);
        conf.setInt("BufferCapacity", 1024 * 1024);

        node.setup(conf);

        final AtomicBoolean inactive = new AtomicBoolean(true);
        new Thread() {
            @Override
            public void run() {
                inactive.set(false);
                node.activate(new NodeContext("LoadGeneratorTestNode"));
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
        for (int i = 0; i < 300; i++) {
            mses.tick(1);
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                LOG.error("Unexpected error while sleeping for 1 s", e);
            }
        }
        node.deactivate();

        // Let the reciever get the tuples from the queue
        for (int i = 0; i <500; i++) {
            mses.tick(1);
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                LOG.error("Unexpected error while sleeping for 1 s", e);
            }
        }

        LOG.debug("Processed {} tuples", lgenSink.count);
                for (Map.Entry<String, Integer> e: lgenSink.collectedTuples.entrySet()) {
            LOG.debug("{} tuples for key {}", e.getValue().intValue(), e.getKey());
        }
    }
}
