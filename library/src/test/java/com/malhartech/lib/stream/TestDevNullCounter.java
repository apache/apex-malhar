/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.lib.stream.DevNullCounter;
import com.malhartech.dag.Component;
import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.ModuleContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.lib.testbench.LoadGenerator;
import com.malhartech.lib.testbench.ThroughputCounter;
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
 * Functional tests for {@link com.malhartech.lib.testbench.DevNullCounter}. <p>
 * <br>
 * node.process is called a billion times<br>
 * With extremely high throughput it does not impact the performance of any other node
 * <br>
 *  Benchmarks:<br>
 * Object payload benchmarked at over 125 Million/sec
 * <br>
 * DRC checks are validated<br>
 *
 */
public class TestDevNullCounter {

    private static Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);

    /**
     * Test configuration and parameter validation of the node
     */
    @Test
    public void testNodeValidation() {

        ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
        DevNullCounter node = new DevNullCounter();

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
    DevNullCounter node = new DevNullCounter();
    ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());

    conf.set(ThroughputCounter.ROLLING_WINDOW_COUNT, "5");
    node.setup(conf);

    node.beginWindow();
    long numtuples = 1000000000;
    Object o = new Object();
    for (long i = 0; i < numtuples; i++) {
      node.process(o);
    }
    node.endWindow();
    LOG.info(String.format("\n*******************************************************\nnumtuples(%d)",  numtuples));
  }
}
