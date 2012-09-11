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
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestLoadRandomGenerator {

    private static Logger LOG = LoggerFactory.getLogger(LoadRandomGenerator.class);

    class TestSink implements Sink {

        HashMap<String, Object> collectedTuples = new HashMap<String, Object>();


        //DefaultSerDe serde = new DefaultSerDe();
        int count = 0;
        boolean test_integer = false;

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
                if (test_integer) {
                  collectedTuples.put(((Integer) payload).toString(), null);
                }
                else {
                  collectedTuples.put((String) payload, null);
                }
                count++;
             }
        }
    }

    /**
     * Test configuration and parameter validation of the node
     */
    @Test
    public void testNodeValidation() {

        NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());
        LoadRandomGenerator node = new LoadRandomGenerator();

        conf.set(LoadRandomGenerator.KEY_MIN_VALUE, "a");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadRandomGenerator.KEY_MIN_VALUE);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadRandomGenerator.KEY_MIN_VALUE,
                    e.getMessage().contains("min_value should be an integer"));
        }

        conf.set(LoadRandomGenerator.KEY_MIN_VALUE, "0");
        conf.set(LoadRandomGenerator.KEY_MAX_VALUE, "b");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadRandomGenerator.KEY_MAX_VALUE);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadRandomGenerator.KEY_MAX_VALUE,
                    e.getMessage().contains("max_value should be an integer"));
        }

        conf.set(LoadRandomGenerator.KEY_MIN_VALUE, "0");
        conf.set(LoadRandomGenerator.KEY_MAX_VALUE, "50");
        conf.set(LoadRandomGenerator.KEY_TUPLES_PER_SEC, "-1");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadRandomGenerator.KEY_TUPLES_PER_SEC);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadRandomGenerator.KEY_TUPLES_PER_SEC,
                    e.getMessage().contains("has to be > 0"));
        }
    }

    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() {

        final LoadRandomGenerator node = new LoadRandomGenerator();
        final ManualScheduledExecutorService mses = new ManualScheduledExecutorService(1);
        final WindowGenerator wingen = new WindowGenerator(mses);

        Configuration config = new Configuration();
        config.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, 0);
        config.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 1);
        wingen.setup(config);

        Sink input = node.connect(Component.INPUT, wingen);
        wingen.connect("mytestnode", input);

        TestSink lgenSink = new TestSink();
        node.connect(LoadRandomGenerator.OPORT_DATA, lgenSink);
        NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());
        lgenSink.test_integer = false; // Testing String for now

        conf.set(LoadRandomGenerator.KEY_MIN_VALUE, "0");
        conf.set(LoadRandomGenerator.KEY_MAX_VALUE, "1000");
        conf.setInt(LoadRandomGenerator.KEY_TUPLES_PER_SEC, 50000000);
        conf.set(LoadRandomGenerator.KEY_STRING_SCHEMA, "true");

        conf.setInt("SpinMillis", 2);
        conf.setInt("BufferCapacity", 1024 * 1024);

        node.setup(conf);

        final AtomicBoolean inactive = new AtomicBoolean(true);
        new Thread() {
            @Override
            public void run() {
                inactive.set(false);
                node.activate(new NodeContext("LoadRandomGeneratorTestNode"));
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
        for (int i = 0; i < 50; i++) {
            mses.tick(1);
            try {
                Thread.sleep(4);
            } catch (InterruptedException e) {
                LOG.error("Unexpected error while sleeping for 1 s", e);
            }
        }
        node.deactivate();

        // Let the reciever get the tuples from the queue
        for (int i = 0; i < 100; i++) {
            mses.tick(1);
            try {
                Thread.sleep(4);
            } catch (InterruptedException e) {
                LOG.error("Unexpected error while sleeping for 1 s", e);
            }
        }

        LOG.debug("Processed {} tuples and {} unique strings", lgenSink.count, lgenSink.collectedTuples.size());
    }
}
