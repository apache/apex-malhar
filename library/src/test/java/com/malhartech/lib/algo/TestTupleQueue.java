/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.testbench.*;
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
 * Benchmarks:<br>
 * String schema generates over 11 Million tuples/sec<br>
 * HashMap schema generates over 1.7 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 *
 */
public class TestTupleQueue
{
  private static Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);

  class TestTupleQueueSink implements Sink
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
      }
    }
  }

  /**
   * Test configuration and parameter validation of the node
   */
  @Test
  public void testNodeValidation()
  {

    ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
    TupleQueue node = new TupleQueue();

    conf.set(TupleQueue.KEY_DEPTH, "aa");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + TupleQueue.KEY_DEPTH);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + TupleQueue.KEY_DEPTH,
                        e.getMessage().contains("has to be an integer"));
    }
  }

  /**
   * Tests both string and non string schema
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    final TupleQueue node = new TupleQueue();

    TestTupleQueueSink queueSink = new TestTupleQueueSink();
    node.connect(FilterClassifier.OPORT_OUT_DATA, queueSink);

    ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
    conf.set(TupleQueue.KEY_DEPTH, "10");
    node.setup(conf);

    final AtomicBoolean inactive = new AtomicBoolean(true);
    new Thread()
    {
      @Override
      public void run()
      {
        inactive.set(false);
        node.activate(new ModuleContext("TupleQueueTestNode", this));
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


  }
}
