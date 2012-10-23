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
   * Tests both string and non string schema
   */
  @Test
  public void testSingleSchemaNodeProcessing() throws Exception
  {
    DevNullCounter node = new DevNullCounter();
    node.setup(new OperatorConfiguration());
    node.setRollingwindowcount(5);


    node.beginWindow();
    long numtuples = 1000000000;
    Object o = new Object();
    for (long i = 0; i < numtuples; i++) {
      node.data.process(o);
    }
    node.endWindow();
    LOG.info(String.format("\n*******************************************************\nnumtuples(%d)",  numtuples));
  }
}
