/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional test for {@link com.malhartech.lib.testbench.LoadRandomClassifier}<p>
 * <br>
 * Four keys are sent in at a high throughput rate and the classification is expected to be cover all combinations<br>
 * <br>
 * Benchmarks: A total of 40 million tuples are pushed in each benchmark<br>
 * String schema does about 1.5 Million tuples/sec<br>
 * LoadRandomClassifier.valueData schema is about 4 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 */
public class TestLoadRandomClassifier {

    private static Logger LOG = LoggerFactory.getLogger(LoadRandomClassifier.class);

    class TestSink implements Sink {

      HashMap<String, HashMap>collectedTuples = new HashMap<String, HashMap>();
      int count = 0;
      boolean isstring = true;

        /**
         *
         * @param payload
         */
        @Override
        public void process(Object payload) {
            if (payload instanceof Tuple) {
                // LOG.debug(payload.toString());
            } else {
                HashMap<String, ArrayList> tuples = (HashMap<String, ArrayList>) payload;
                for (Map.Entry<String, ArrayList> e : tuples.entrySet()) {
                    HashMap<String, Object> val = collectedTuples.get(e.getKey());
                    if (val == null) {
                      val = new HashMap<String, Object>();
                      collectedTuples.put(e.getKey(), val);
                    }
                    if (isstring) {
                      ArrayList<String> alist = e.getValue();
                      for (String s : alist) { // These string should be ikey:val
                        val.put(s, null);
                      }
                    }
                    else { // tbd, convert valueData into a string
                      ArrayList<LoadRandomClassifier.valueData> alist = e.getValue();
                      for (LoadRandomClassifier.valueData v : alist) {
                        val.put(v.str, null);
                      }
                    }
                    count++;
                }
            }
        }
    }

    /**
     * Test configuration and parameter validation of the node
     */
    @Test
    public void testNodeValidation() {

        NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());
        LoadRandomClassifier node = new LoadRandomClassifier();

       // conf.set(LoadRandomClassifier.KEY_KEYS, "x:0,100;y:0,100;gender:0,1;age:10,120"); // the good key
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
  public void testNodeProcessing () throws Exception {
    testSchemaNodeProcessing(true);
    testSchemaNodeProcessing(false);
  }



    public void testSchemaNodeProcessing(boolean isstring) throws Exception {

        LoadRandomClassifier node = new LoadRandomClassifier();

        TestSink classifySink = new TestSink();
        node.connect(LoadClassifier.OPORT_OUT_DATA, classifySink);

        NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());
        conf.set(LoadRandomClassifier.KEY_KEYS, "x:0,99;y:0,99;gender:0,1;age:10,109"); // the good key

        if (isstring) {
          conf.set(LoadRandomClassifier.KEY_STRING_SCHEMA, "true");
        }
        else {
          conf.set(LoadRandomClassifier.KEY_STRING_SCHEMA, "false");
        }
        classifySink.isstring = isstring;

        conf.setInt("SpinMillis", 10);
        conf.setInt("BufferCapacity", 1024 * 1024);
        node.setup(conf);

        int sentval = 0;
        ArrayList<String> alist = new ArrayList<String>();
        alist.add("key1");
        alist.add("key2");
        alist.add("key3");
        alist.add("key4");
        for (int i = 0; i < 10000000; i++) {
          if (isstring) {
            node.process("key1");
            node.process("key2");
            node.process("key3");
            node.process("key4");
          }
          else {
            node.process(alist);
          }
           sentval += 4;
        }
        node.endWindow();


        LOG.info(String.format("\nThe number of tuples sent (%d) and processed(%d) with stringschema as %s",
                sentval,
                classifySink.count,
                isstring ? "true": "false"));

        for (Map.Entry<String, HashMap> e : classifySink.collectedTuples.entrySet()) {
          LOG.info(String.format("Key %s has %d entries", e.getKey(), e.getValue().size()));
        }
        Assert.assertEquals("number emitted tuples", sentval, classifySink.count);
    }
}

