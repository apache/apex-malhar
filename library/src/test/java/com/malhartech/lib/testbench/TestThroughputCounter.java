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
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.testbench.LoadGenerator}. <p>
 * <br>
 * Load is generated and the tuples are outputted to ensure that the numbers are roughly in line with the weights<br>
 * <br>
 *  Benchmarks:<br>
 * String schema generates over 11 Million tuples/sec<br>
 * HashMap schema generates over 1.7 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 *
 */
public class TestThroughputCounter {

    private static Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);

  class TestCountSink implements Sink
  {
    long count = 0;
    long average = 0;

    /**
     * @param payload
     */
    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
        // LOG.debug(payload.toString());
      }
      else {
        LOG.debug("Got a tuple");
        HashMap<String, Number> tuples = (HashMap<String, Number>)payload;
        average = ((Long) tuples.get(ThroughputCounter.OPORT_COUNT_TUPLE_AVERAGE)).longValue();
        count += ((Long) tuples.get(ThroughputCounter.OPORT_COUNT_TUPLE_COUNT)).longValue();
      }
    }
  }

    /**
     * Test configuration and parameter validation of the node
     */
    @Test
    public void testNodeValidation() {

        ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
        ThroughputCounter node = new ThroughputCounter();

        conf.set(ThroughputCounter.ROLLING_WINDOW_COUNT, "aa");
        String rstr = conf.get(ThroughputCounter.ROLLING_WINDOW_COUNT);
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + ThroughputCounter.ROLLING_WINDOW_COUNT);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + ThroughputCounter.ROLLING_WINDOW_COUNT,
                    e.getMessage().contains("has to be an integer"));
        }
    }

  /**
   * Tests both string and non string schema
   */
  @Test
  public void testSingleSchemaNodeProcessing() throws Exception
  {
    ThroughputCounter node = new ThroughputCounter();

    TestCountSink countSink = new TestCountSink();
    node.connect(FilterClassifier.OPORT_OUT_DATA, countSink);
    ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());

    conf.set(ThroughputCounter.ROLLING_WINDOW_COUNT, "5");

    try {
      node.setup(conf);
    }
    catch (IllegalArgumentException e) {;
    }

    node.beginWindow();
    HashMap<String, Integer> input;
    int aint = 1000;
    int bint = 100;
    Integer aval = new Integer(aint);
    Integer bval = new Integer(bint);
    int numtuples = 1000000;
    int sentval = 0;
    for (int i = 0; i < numtuples; i++) {
      input = new HashMap<String, Integer>();
      input.put("a", aval);
      input.put("b", bval);
      sentval += 2;
      node.process(input);
    }
    node.endWindow();

    try {
      for (int i = 0; i < 10; i++) {
        Thread.sleep(5);
      }
    }
    catch (InterruptedException ex) {
      LOG.debug(ex.getLocalizedMessage());
    }

    LOG.info(String.format("\n*******************************************************\nGot average per sec(%d), count(got %d, expected %d), numtuples(%d)",
                           countSink.average,
                           countSink.count,
                           (aint+bint) * numtuples,
                           sentval));
  }
}
