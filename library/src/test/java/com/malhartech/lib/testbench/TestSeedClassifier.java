/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.esotericsoftware.minlog.Log;
import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.ModuleContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.lib.math.ArithmeticQuotient;
import com.malhartech.stream.StramTestSupport;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.malhartech.lib.testbench.SeedClassifier} for three configuration><p>
 * <br>
 * Configuration 1: Provide values and weights<br>
 * Configuration 2: Provide values but no weights (even weights)<br>
 * Configuration 3: Provide no values or weights<br>
 * <br>
 * Benchmarks: Currently does about 3 Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 * Validates all DRC checks of the node<br>
 */
public class TestSeedClassifier {

    private static Logger LOG = LoggerFactory.getLogger(LoadClassifier.class);

    class TestSink implements Sink {

        HashMap<String, Integer> collectedTuples = new HashMap<String, Integer>();
        HashMap<String, Double> collectedTupleValues = new HashMap<String, Double>();

        boolean isstring = true;
        int count = 0;

        /**
         *
         * @param payload
         */
        @Override
        public void process(Object payload) {
            if (payload instanceof Tuple) {
                // LOG.debug(payload.toString());
            } else {
              if (isstring) {
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

        ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
        SeedClassifier node = new SeedClassifier();
        // String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        // String[] vstr = config.getTrimmedStrings(KEY_VALUES);


        conf.set(SeedClassifier.KEY_SEED_START, "");
        conf.set(SeedClassifier.KEY_SEED_END, "100");

        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + SeedClassifier.KEY_SEED_START);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + SeedClassifier.KEY_SEED_START,
                    e.getMessage().contains("seedstart is empty, but seedend"));
        }

        conf.set(SeedClassifier.KEY_SEED_START, "1");
        conf.set(SeedClassifier.KEY_SEED_END, "");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + SeedClassifier.KEY_SEED_END);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + SeedClassifier.KEY_SEED_END,
                    e.getMessage().contains("but seedend is empty"));
        }

        conf.set(SeedClassifier.KEY_SEED_START, "1");
        conf.set(SeedClassifier.KEY_SEED_END, "a");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + SeedClassifier.KEY_SEED_END);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + SeedClassifier.KEY_SEED_END,
                    e.getMessage().contains("should be an integer"));
        }

        conf.set(SeedClassifier.KEY_SEED_START, "a");
        conf.set(SeedClassifier.KEY_SEED_END, "1");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + SeedClassifier.KEY_SEED_START);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + SeedClassifier.KEY_SEED_START,
                    e.getMessage().contains("should be an integer"));
        }
    }

     /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() throws Exception {
      testSchemaNodeProcessing(true); // 5.9 million/sec
      testSchemaNodeProcessing(false); // 4.4 million/sec
    }

    /**
     * Test node logic emits correct results
     */
    public void testSchemaNodeProcessing(boolean isstring) throws Exception {

      final SeedClassifier node = new SeedClassifier();

      TestSink classifySink = new TestSink();

      Sink inSink1 = node.connect(SeedClassifier.IPORT_IN_DATA1, node);
      Sink inSink2 = node.connect(SeedClassifier.IPORT_IN_DATA2, node);
      node.connect(SeedClassifier.OPORT_OUT_DATA, classifySink);

      ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());

      conf.set(SeedClassifier.KEY_SEED_START, "1");
      conf.set(SeedClassifier.KEY_SEED_END, "1000000");
      conf.set(SeedClassifier.KEY_IN_DATA1_CLASSIFIER, "x");
      conf.set(SeedClassifier.KEY_IN_DATA2_CLASSIFIER, "y");
      conf.set(SeedClassifier.KEY_STRING_SCHEMA, isstring ? "true" : "false");

      node.setSpinMillis(10);
      node.setBufferCapacity(1024 * 1024);
      node.setup(conf);

      final AtomicBoolean inactive = new AtomicBoolean(true);
      new Thread()
      {
        @Override
        public void run()
        {
          inactive.set(false);
          node.activate(new ModuleContext("SeedClassifierTestNode", this));
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
      if (isstring) {
        String input;
        for (int i = 0; i < numtuples; i++) {
          input = Integer.toString(i);
          inSink1.process(input);
          inSink2.process(input);
        }
      }
      else {
        Integer input;
        for (int i = 0; i < numtuples; i++) {
          input  = new Integer(i);
          inSink1.process(input);
          inSink2.process(input);
        }
      }

      Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
      inSink1.process(et);
      inSink2.process(et);

      // Should get one bag of keys "a", "b", "c"
      try {
        for (int i = 0; i < 50; i++) {
          Thread.sleep(10);
          if (classifySink.count >= numtuples*2 - 1) {
            break;
          }
        }
      }
      catch (InterruptedException ex) {
        LOG.debug(ex.getLocalizedMessage());
      }

      // One for each key
      Assert.assertEquals("number emitted tuples", numtuples*2, classifySink.count);
      LOG.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", classifySink.count));
    }
}
