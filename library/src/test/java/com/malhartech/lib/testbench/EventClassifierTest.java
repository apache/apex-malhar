/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.esotericsoftware.minlog.Log;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.malhartech.lib.testbench.EventClassifier} for three configuration><p>
 * <br>
 * Configuration 1: Provide values and weights<br>
 * Configuration 2: Provide values but no weights (even weights)<br>
 * Configuration 3: Provide no values or weights<br>
 * <br>
 * Benchmarks: Currently does about 3 Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 * Validates all DRC checks of the node<br>
 */
public class EventClassifierTest {

    private static Logger LOG = LoggerFactory.getLogger(EventClassifier.class);

    class TestSink implements Sink {

        HashMap<String, Integer> collectedTuples = new HashMap<String, Integer>();
        HashMap<String, Double> collectedTupleValues = new HashMap<String, Double>();

        int count = 0;
        boolean dohash = true;

        /**
         *
         * @param payload
         */
        @Override
        public void process(Object payload) {
            if (payload instanceof Tuple) {
                // LOG.debug(payload.toString());
            } else {
              count++;
              if (dohash) {
                HashMap<String, Double> tuple = (HashMap<String, Double>)payload;
                for (Map.Entry<String, Double> e: tuple.entrySet()) {
                  Integer ival = collectedTuples.get(e.getKey());
                  if (ival == null) {
                    ival = new Integer(1);
                  }
                  else {
                    ival = ival + 1;
                  }
                  collectedTuples.put(e.getKey(), ival);
                  collectedTupleValues.put(e.getKey(), e.getValue());
                }
              }
            }
        }
        /**
         *
         */
        public void clear() {
            collectedTuples.clear();
            collectedTupleValues.clear();
            count = 0;
        }
    }

    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() throws Exception
    {

      EventClassifier node = new EventClassifier();
      TestSink classifySink = new TestSink();
      classifySink.dohash = true;
      node.data.setSink(classifySink);

      HashMap<String, Double> keymap = new HashMap<String, Double>();
      keymap.put("a", 1.0);
      keymap.put("b", 4.0);
      keymap.put("c", 5.0);
      node.setKeyMap(keymap);
      node.setOperationReplace();

      HashMap<String, ArrayList<Integer>> wmap = new HashMap<String, ArrayList<Integer>>();
      ArrayList<Integer> list = new ArrayList<Integer>(3);
      list.add(60);
      list.add(10);
      list.add(35);
      wmap.put("ia", list);
      list = new ArrayList<Integer>(3);
      list.add(10);
      list.add(75);
      list.add(15);
      wmap.put("ib", list);
      list = new ArrayList<Integer>(3);
      list.add(20);
      list.add(10);
      list.add(70);
      wmap.put("ic", list);
      list = new ArrayList<Integer>(3);
      list.add(50);
      list.add(15);
      list.add(35);
      wmap.put("id", list);
      node.setKeyWeights(wmap);
      node.setup(new OperatorConfiguration());

      HashMap<String, Double> input = new HashMap<String, Double>();
      int sentval = 0;
      for (int i = 0; i < 1000000; i++) {
        input.clear();
        input.put("ia", 2.0);
        input.put("ib", 20.0);
        input.put("ic", 1000.0);
        input.put("id", 1000.0);
        sentval += 4;
        node.event.process(input);
      }
      node.endWindow();
      int ival = 0;
      if (classifySink.dohash) {
        for (Map.Entry<String, Integer> e: classifySink.collectedTuples.entrySet()) {
          ival += e.getValue().intValue();
        }
      }
      else {
        ival = classifySink.count;
      }

      LOG.info(String.format("\nThe number of keys in %d tuples are %d and %d",
                             ival,
                             classifySink.collectedTuples.size(),
                             classifySink.collectedTupleValues.size()));
      for (Map.Entry<String, Double> ve: classifySink.collectedTupleValues.entrySet()) {
        Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
        Log.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
      }
      Assert.assertEquals("number emitted tuples", sentval, ival);

      // Now test a node with no weights
      EventClassifier nwnode = new EventClassifier();
      classifySink.clear();
      nwnode.data.setSink(classifySink);
      nwnode.setKeyMap(keymap);
      nwnode.setOperationReplace();
      nwnode.setup(new OperatorConfiguration());

      sentval = 0;
      for (int i = 0; i < 1000000; i++) {
        input.clear();
        input.put("ia", 2.0);
        input.put("ib", 20.0);
        input.put("ic", 1000.0);
        input.put("id", 1000.0);
        sentval += 4;
        nwnode.event.process(input);
      }
      nwnode.endWindow();
      ival = 0;
      if (classifySink.dohash) {
        for (Map.Entry<String, Integer> e: classifySink.collectedTuples.entrySet()) {
          ival += e.getValue().intValue();
        }
      }
      else {
        ival = classifySink.count;
      }
      LOG.info(String.format("\nThe number of keys in %d tuples are %d and %d",
                             ival,
                             classifySink.collectedTuples.size(),
                             classifySink.collectedTupleValues.size()));
      for (Map.Entry<String, Double> ve: classifySink.collectedTupleValues.entrySet()) {
        Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
        Log.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
      }
      Assert.assertEquals("number emitted tuples", sentval, ival);


      // Now test a node with no weights and no values
      EventClassifier nvnode = new EventClassifier();
      classifySink.clear();
      keymap.put("a", 0.0);
      keymap.put("b", 0.0);
      keymap.put("c", 0.0);

      nvnode.data.setSink(classifySink);
      nwnode.setKeyMap(keymap);
      nvnode.setOperationReplace();
      nvnode.setup(new OperatorConfiguration());

      sentval = 0;
      for (int i = 0; i < 1000000; i++) {
        input.clear();
        input.put("ia", 2.0);
        input.put("ib", 20.0);
        input.put("ic", 500.0);
        input.put("id", 1000.0);
        sentval += 4;
        nvnode.event.process(input);
      }
    nvnode.endWindow();
    ival = 0;
    if (classifySink.dohash) {
      for (Map.Entry<String, Integer> e: classifySink.collectedTuples.entrySet()) {
        ival += e.getValue().intValue();
      }
    }
    else {
      ival = classifySink.count;
    }
    LOG.info(String.format("\nThe number of keys in %d tuples are %d and %d",
                           ival,
                           classifySink.collectedTuples.size(),
                           classifySink.collectedTupleValues.size()));
    for (Map.Entry<String, Double> ve: classifySink.collectedTupleValues.entrySet()) {
      Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
      Log.info(String.format("%d tuples of key \"%s\" has value %f",
                             ieval.intValue(),
                             ve.getKey(),
                             ve.getValue()));
    }
    Assert.assertEquals("number emitted tuples", sentval, ival);
  }
}
