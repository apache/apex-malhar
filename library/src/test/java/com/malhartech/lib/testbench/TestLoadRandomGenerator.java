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
 * Functional test for {@link com.malhartech.lib.testbench.LoadRandomGenerator}<p>
 * <br>
 * Tests both string and integer. Sets range to 0 to 999 and generates random numbers. With millions
 * of tuple all the values are covered<br>
 * <br>
 * Benchmark: pushes as many tuples are possible<br>
 * String schema does about 3 Million tuples/sec<br>
 * Integer schema does about 7 Million tuples/sec<br>
 * <br>
 * DRC validation is done<br>
 * <br>
 */

public class TestLoadRandomGenerator {

    private static Logger LOG = LoggerFactory.getLogger(LoadRandomGenerator.class);

    class TestSink implements Sink {

        HashMap<Object, Object> collectedTuples = new HashMap<Object, Object>();


        //DefaultSerDe serde = new DefaultSerDe();
        int count = 0;
        boolean isstring = false;

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
                if (isstring) {
                  collectedTuples.put((String) payload, null);
                }
                else {
                  collectedTuples.put((Integer) payload, null);
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
        LOG.debug("Testing Node Validation: start");

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
        conf.set(LoadRandomGenerator.KEY_TUPLES_BLAST, "-1");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadRandomGenerator.KEY_TUPLES_BLAST);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadRandomGenerator.KEY_TUPLES_BLAST,
                    e.getMessage().contains("has to be > 0"));
        }

        conf.set(LoadRandomGenerator.KEY_TUPLES_BLAST, "1000");
        conf.set(LoadRandomGenerator.KEY_SLEEP_TIME, "-1");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadRandomGenerator.KEY_SLEEP_TIME);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadRandomGenerator.KEY_SLEEP_TIME,
                    e.getMessage().contains("has to be > 0"));
        }
        LOG.debug("Testing Node Validation: end");
    }

    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing () throws Exception {
      testSchemaNodeProcessing(true);
      testSchemaNodeProcessing(false);
    }


    public void testSchemaNodeProcessing(boolean isstring) throws Exception {

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
        lgenSink.isstring = isstring;

        conf.set(LoadRandomGenerator.KEY_MIN_VALUE, "0");
        conf.set(LoadRandomGenerator.KEY_MAX_VALUE, "1000");
        conf.setInt(LoadRandomGenerator.KEY_TUPLES_BLAST, 50000000);
        conf.setInt(LoadRandomGenerator.KEY_SLEEP_TIME, 1);

        if (isstring) {
          conf.set(LoadRandomGenerator.KEY_STRING_SCHEMA, "true");
        }

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
        for (int i = 0; i < 100; i++) {
            mses.tick(1);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                LOG.error("Unexpected error while sleeping for 1 s", e);
            }
        }
        node.deactivate();

        // Let the reciever get the tuples from the queue
        for (int i = 0; i < 100; i++) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                LOG.error("Unexpected error while sleeping for 1 s", e);
            }
        }

        LOG.debug("Processed {} tuples and {} unique strings", lgenSink.count, lgenSink.collectedTuples.size());
    }
}
