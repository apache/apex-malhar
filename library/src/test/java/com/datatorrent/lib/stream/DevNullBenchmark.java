/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.testbench.EventGenerator;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.testbench.DevNull}. <p>

 *
 */
public class DevNullBenchmark
{
  private static Logger log = LoggerFactory.getLogger(DevNull.class);

  /**
   * Tests both string and non string schema
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testSingleSchemaNodeProcessing() throws Exception
  {
    DevNull oper = new DevNull();

    oper.beginWindow(0);
    long numtuples = 100000000;
    Object o = new Object();
    for (long i = 0; i < numtuples; i++) {
      oper.data.process(o);
    }
    oper.endWindow();
    log.info(String.format("\n*******************************************************\nnumtuples(%d)", numtuples));
  }
}
