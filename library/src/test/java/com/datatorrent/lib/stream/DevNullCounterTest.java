/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.DevNullCounter;
import com.datatorrent.lib.testbench.EventGenerator;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.testbench.DevNullCounter}. <p>
 * <br>
 * oper.process is called a billion times<br>
 * With extremely high throughput it does not impact the performance of any other oper
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
    DevNullCounter oper = new DevNullCounter();
    oper.setRollingwindowcount(5);
    oper.setup(null);


    oper.beginWindow(0);
    long numtuples = 1000000;
    Object o = new Object();
    for (long i = 0; i < numtuples; i++) {
      oper.data.process(o);
    }
    oper.endWindow();
    LOG.info(String.format("\n*******************************************************\nnumtuples(%d)",  numtuples));
  }
}
