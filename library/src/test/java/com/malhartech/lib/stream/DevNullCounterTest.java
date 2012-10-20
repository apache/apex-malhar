/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.lib.testbench.EventGenerator;
import com.malhartech.lib.testbench.ThroughputCounter;
import java.util.HashMap;
import junit.framework.Assert;
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
public class DevNullCounterTest {

    private static Logger LOG = LoggerFactory.getLogger(EventGenerator.class);

    /**
     * Test configuration and parameter validation of the node
     */
    @Test
    public void testNodeValidation() {

        OperatorConfiguration conf = new OperatorConfiguration("mynode", new HashMap<String, String>());
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
    OperatorConfiguration conf = new OperatorConfiguration("mynode", new HashMap<String, String>());

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
