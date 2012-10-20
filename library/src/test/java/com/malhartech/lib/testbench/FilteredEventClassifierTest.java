/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.esotericsoftware.minlog.Log;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.malhartech.lib.testbench.FilteredEventClassifier} for three configuration><p>
 * <br>
 * Configuration 1: Provide values and weights<br>
 * Configuration 2: Provide values but no weights (even weights)<br>
 * Configuration 3: Provide no values or weights<br>
 * <br>
 * Benchmarks: Currently handle about 20 Million tuples/sec incoming tuples in debugging environment. Need to test on larger nodes<br>
 * <br>
 * Validates all DRC checks of the node<br>
 */
public class FilteredEventClassifierTest {

    private static Logger LOG = LoggerFactory.getLogger(FilteredEventClassifier.class);

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

        OperatorConfiguration conf = new OperatorConfiguration("mynode", new HashMap<String, String>());
        FilterClassifier node = new FilterClassifier();
        // String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        // String[] vstr = config.getTrimmedStrings(KEY_VALUES);


        conf.set(FilteredEventClassifier.KEY_KEYS, "");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + FilteredEventClassifier.KEY_KEYS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + FilteredEventClassifier.KEY_KEYS,
                    e.getMessage().contains("is empty"));
        }

        conf.set(FilteredEventClassifier.KEY_KEYS, "a,b,c"); // from now on keys are a,b,c
        conf.set(FilteredEventClassifier.KEY_FILTER, "");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + FilteredEventClassifier.KEY_FILTER);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + FilteredEventClassifier.KEY_FILTER,
                    e.getMessage().contains("is empty"));
        }
        conf.set(FilteredEventClassifier.KEY_FILTER, "22");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + FilteredEventClassifier.KEY_FILTER);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + FilteredEventClassifier.KEY_FILTER,
                    e.getMessage().contains("has wrong format"));
        }
        conf.set(FilteredEventClassifier.KEY_FILTER, "2,a");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + FilteredEventClassifier.KEY_FILTER);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + FilteredEventClassifier.KEY_FILTER,
                    e.getMessage().contains("Filter string should be an integer"));
        }
        conf.set(FilteredEventClassifier.KEY_FILTER, "7,1000");

        conf.set(FilteredEventClassifier.KEY_VALUES, "1,2,3,4");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + FilteredEventClassifier.KEY_VALUES);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + FilteredEventClassifier.KEY_VALUES,
                    e.getMessage().contains("does not match number of keys"));
        }

        conf.set(FilteredEventClassifier.KEY_VALUES, "1,2a,3");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + FilteredEventClassifier.KEY_VALUES);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + FilteredEventClassifier.KEY_VALUES,
                    e.getMessage().contains("Value string should be float"));
        }

        conf.set(FilteredEventClassifier.KEY_VALUES, "1,2,3");
        conf.set(FilteredEventClassifier.KEY_WEIGHTS, "ia:60,10,35;;ic:20,10,70;id:50,15,35");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + FilteredEventClassifier.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + FilteredEventClassifier.KEY_WEIGHTS,
                    e.getMessage().contains("One of the keys in"));
        }

        conf.set(FilteredEventClassifier.KEY_WEIGHTS, "ia:60,10,35;ib,10,75,15;ic:20,10,70;id:50,15,35");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + FilteredEventClassifier.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + FilteredEventClassifier.KEY_WEIGHTS,
                    e.getMessage().contains("need two strings separated by"));
        }

        conf.set(FilteredEventClassifier.KEY_WEIGHTS, "ia:60,10,35;ib:10,75;ic:20,10,70;id:50,15,35");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + FilteredEventClassifier.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + FilteredEventClassifier.KEY_WEIGHTS,
                    e.getMessage().contains("does not match the number of keys"));
        }

        conf.set(FilteredEventClassifier.KEY_WEIGHTS, "ia:60,10,35;ib:10,75,1a5;ic:20,10,70;id:50,15,35");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + FilteredEventClassifier.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + FilteredEventClassifier.KEY_WEIGHTS,
                    e.getMessage().contains("Weight string should be an integer"));
        }
    }

    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() throws Exception {

        FilteredEventClassifier node = new FilteredEventClassifier();

        TestSink classifySink = new TestSink();
        node.connect(FilterClassifier.OPORT_OUT_DATA, classifySink);
         OperatorConfiguration conf = new OperatorConfiguration("mynode", new HashMap<String, String>());

        conf.set(FilteredEventClassifier.KEY_KEYS, "a,b,c");
        conf.set(FilteredEventClassifier.KEY_VALUES, "1,4,5");
        conf.set(FilteredEventClassifier.KEY_WEIGHTS, "ia:60,10,35;ib:10,75,15;ic:20,10,70;id:50,15,35");
        conf.set(FilteredEventClassifier.KEY_FILTER, "10,1000");

        node.setSpinMillis(10);
        node.setBufferCapacity(1024 * 1024);
        try {
          node.setup(conf);
        } catch (IllegalArgumentException e) {;}

        HashMap<String, Double> input = new HashMap<String, Double>();
        int sentval = 0;
        for (int i = 0; i < 1000000; i++) {
            input.clear();
            input.put("a,ia", 2.0);
            input.put("a,ib", 2.0);
            input.put("a,ic", 2.0);
            input.put("a,id", 2.0);
            input.put("b,ia", 2.0);
            input.put("b,ib", 2.0);
            input.put("b,ic", 2.0);
            input.put("b,id", 2.0);
            input.put("c,ia", 2.0);
            input.put("c,ib", 2.0);
            input.put("c,ic", 2.0);
            input.put("c,id", 2.0);

            sentval += 12;
            node.process(input);
        }
        node.endWindow();
        int ival = 0;
        for (Map.Entry<String, Integer> e : classifySink.collectedTuples.entrySet()) {
            ival += e.getValue().intValue();
        }

        LOG.info(String.format("\n*******************************************************\nFiltered %d out of %d intuples with %d and %d unique keys",
                ival,
                sentval,
                classifySink.collectedTuples.size(),
                classifySink.collectedTupleValues.size()));
        for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
            Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
            Log.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
        }

        // Now test a node with no weights
        FilteredEventClassifier nwnode = new FilteredEventClassifier();
        classifySink.clear();
        nwnode.connect(FilteredEventClassifier.OPORT_OUT_DATA, classifySink);
        conf.set(FilteredEventClassifier.KEY_WEIGHTS, "");
        nwnode.setup(conf);

        sentval = 0;
        for (int i = 0; i < 1000000; i++) {
            input.clear();
            input.put("a,ia", 2.0);
            input.put("a,ib", 2.0);
            input.put("a,ic", 2.0);
            input.put("a,id", 2.0);
            input.put("b,ia", 2.0);
            input.put("b,ib", 2.0);
            input.put("b,ic", 2.0);
            input.put("b,id", 2.0);
            input.put("c,ia", 2.0);
            input.put("c,ib", 2.0);
            input.put("c,ic", 2.0);
            input.put("c,id", 2.0);

            sentval += 12;
            nwnode.process(input);
        }
        nwnode.endWindow();
        ival = 0;
        for (Map.Entry<String, Integer> e : classifySink.collectedTuples.entrySet()) {
            ival += e.getValue().intValue();
        }

        LOG.info(String.format("\n*******************************************************\nFiltered %d out of %d intuples with %d and %d unique keys",
                ival,
                sentval,
                classifySink.collectedTuples.size(),
                classifySink.collectedTupleValues.size()));
        for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
            Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
            Log.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
        }

        // Now test a node with no weights and no values
        FilteredEventClassifier nvnode = new FilteredEventClassifier();
        classifySink.clear();
        nvnode.connect(FilteredEventClassifier.OPORT_OUT_DATA, classifySink);
        conf.set(FilteredEventClassifier.KEY_WEIGHTS, "");
        conf.set(FilteredEventClassifier.KEY_VALUES, "");
        nvnode.setup(conf);

        sentval = 0;
        for (int i = 0; i < 1000000; i++) {
            input.clear();
            input.put("a,ia", 2.0);
            input.put("a,ib", 2.0);
            input.put("a,ic", 2.0);
            input.put("a,id", 2.0);
            input.put("b,ia", 2.0);
            input.put("b,ib", 2.0);
            input.put("b,ic", 2.0);
            input.put("b,id", 2.0);
            input.put("c,ia", 2.0);
            input.put("c,ib", 2.0);
            input.put("c,ic", 2.0);
            input.put("c,id", 2.0);

            sentval += 12;
            nvnode.process(input);
        }
        nvnode.endWindow();
        ival = 0;
        for (Map.Entry<String, Integer> e : classifySink.collectedTuples.entrySet()) {
            ival += e.getValue().intValue();
        }
        LOG.info(String.format("\n*******************************************************\nFiltered %d out of %d intuples with %d and %d unique keys",
                ival,
                sentval,
                classifySink.collectedTuples.size(),
                classifySink.collectedTupleValues.size()));

        for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
            Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
            Log.info(String.format("%d tuples of key \"%s\" has value %f",
                    ieval.intValue(),
                    ve.getKey(),
                    ve.getValue()));
        }
    }
}
