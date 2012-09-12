/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.esotericsoftware.minlog.Log;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestLoadRandomClassifier {

    private static Logger LOG = LoggerFactory.getLogger(LoadRandomClassifier.class);

    class TestSink implements Sink {

        HashMap<String, Integer> collectedTuples = new HashMap<String, Integer>();
        HashMap<String, Double> collectedTupleValues = new HashMap<String, Double>();

        /**
         *
         * @param payload
         */
        @Override
        public void process(Object payload) {
            if (payload instanceof Tuple) {
                // LOG.debug(payload.toString());
            } else {
                HashMap<String, Double> tuple = (HashMap<String, Double>) payload;
                for (Map.Entry<String, Double> e : tuple.entrySet()) {
                    Integer ival = collectedTuples.get(e.getKey());
                    if (ival == null) {
                        ival = new Integer(1);
                    } else {
                        ival = ival + 1;
                    }
                    collectedTuples.put(e.getKey(), ival);
                    collectedTupleValues.put(e.getKey(), e.getValue());
                }
            }
        }
        /**
         *
         */
        public void clear() {
            collectedTuples.clear();
            collectedTupleValues.clear();
        }
    }

    /**
     * Test configuration and parameter validation of the node
     */
    @Test
    public void testNodeValidation() {

        NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());
        LoadRandomClassifier node = new LoadRandomClassifier();
        // String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        // String[] vstr = config.getTrimmedStrings(KEY_VALUES);

 //       conf.set(LoadRandomClassifier.KEY_KEYS, "x:0,100;y:0,100;gender:0,1;age:10,120"); // the good key
        conf.set(LoadRandomClassifier.KEY_KEYS, "");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadRandomClassifier.KEY_KEYS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadRandomClassifier.KEY_KEYS,
                    e.getMessage().contains("is empty"));
        }

        conf.set(LoadRandomClassifier.KEY_KEYS, "x:0,100;;gender:0,1;age:10,120");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadRandomClassifier.KEY_KEYS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadRandomClassifier.KEY_KEYS,
                    e.getMessage().contains("slot of parameter \"key\" is empty"));
        }

       conf.set(LoadRandomClassifier.KEY_KEYS, "x:0,100;y:0:100;gender:0,1;age:10,120"); // the good key
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadRandomClassifier.KEY_KEYS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadRandomClassifier.KEY_KEYS,
                    e.getMessage().contains("malformed in parameter \"key\""));
        }

       conf.set(LoadRandomClassifier.KEY_KEYS, "x:0,100;y:0,100,3;gender:0,1;age:10,120"); // the good key
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadRandomClassifier.KEY_KEYS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadRandomClassifier.KEY_KEYS,
                    e.getMessage().contains("of parameter \"key\" is malformed"));
        }

       conf.set(LoadRandomClassifier.KEY_KEYS, "x:0,100;y:100,0;gender:0,1;age:10,120"); // the good key
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadRandomClassifier.KEY_KEYS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadRandomClassifier.KEY_KEYS,
                    e.getMessage().contains("Low value \"100\" is >= high value \"0\" for \"y\""));
        }
    }

    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() throws Exception
    {

        LoadRandomClassifier node = new LoadRandomClassifier();

        TestSink classifySink = new TestSink();
        node.connect(LoadClassifier.OPORT_OUT_DATA, classifySink);
        HashMap<String, Double> input = new HashMap<String, Double>();

        NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());

        conf.set(LoadRandomClassifier.KEY_KEYS, "x:0,100;y:0,100;gender:0,1;age:10,120"); // the good key
        conf.set(LoadRandomClassifier.KEY_STRING_SCHEMA, "true");

        conf.setInt("SpinMillis", 10);
        conf.setInt("BufferCapacity", 1024 * 1024);
        node.setup(conf);
/*
        int sentval = 0;
        for (int i = 0; i < 1000000; i++) {
            input.clear();
            input.put("ia", 2.0);
            input.put("ib", 20.0);
            input.put("ic", 1000.0);
            input.put("id", 1000.0);
            sentval += 4;
            node.process(input);
        }
        node.endWindow();
        int ival = 0;
        for (Map.Entry<String, Integer> e : classifySink.collectedTuples.entrySet()) {
            ival += e.getValue().intValue();
        }

        LOG.info(String.format("\nThe number of keys in %d tuples are %d and %d",
                ival,
                classifySink.collectedTuples.size(),
                classifySink.collectedTupleValues.size()));
        for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
            Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
            Log.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
        }
        Assert.assertEquals("number emitted tuples", sentval, ival);
        // Now test a node with no weights
        LoadClassifier nwnode = new LoadClassifier();
        classifySink.clear();
        nwnode.connect(LoadRandomClassifier.OPORT_OUT_DATA, classifySink);
        nwnode.setup(conf);

        sentval = 0;
        for (int i = 0; i < 1000000; i++) {
            input.clear();
            input.put("ia", 2.0);
            input.put("ib", 20.0);
            input.put("ic", 1000.0);
            input.put("id", 1000.0);
            sentval += 4;
            nwnode.process(input);
        }
        nwnode.endWindow();
        ival = 0;
        for (Map.Entry<String, Integer> e : classifySink.collectedTuples.entrySet()) {
            ival += e.getValue().intValue();
        }
        LOG.info(String.format("\nThe number of keys in %d tuples are %d and %d",
                ival,
                classifySink.collectedTuples.size(),
                classifySink.collectedTupleValues.size()));
        for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
            Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
            Log.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
        }
        Assert.assertEquals("number emitted tuples", sentval, ival);


        // Now test a node with no weights and no values
        LoadRandomClassifier nvnode = new LoadRandomClassifier();
        classifySink.clear();
        nvnode.connect(LoadRandomClassifier.OPORT_OUT_DATA, classifySink);
        nvnode.setup(conf);

        sentval = 0;
        for (int i = 0; i < 1000000; i++) {
            input.clear();
            input.put("ia", 2.0);
            input.put("ib", 20.0);
            input.put("ic", 500.0);
            input.put("id", 1000.0);
            sentval += 4;
            nvnode.process(input);
        }
        nvnode.endWindow();
        ival = 0;
        for (Map.Entry<String, Integer> e : classifySink.collectedTuples.entrySet()) {
            ival += e.getValue().intValue();
        }
        LOG.info(String.format("\nThe number of keys in %d tuples are %d and %d",
                ival,
                classifySink.collectedTuples.size(),
                classifySink.collectedTupleValues.size()));
        for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
            Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
            Log.info(String.format("%d tuples of key \"%s\" has value %f",
                    ieval.intValue(),
                    ve.getKey(),
                    ve.getValue()));
        }
        Assert.assertEquals("number emitted tuples", sentval, ival);
        */
    }
}

