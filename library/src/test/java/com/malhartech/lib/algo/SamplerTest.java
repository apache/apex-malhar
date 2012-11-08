/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestCountSink;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.Sampler}<p>
 *
 */
public class SamplerTest
{
  private static Logger log = LoggerFactory.getLogger(SamplerTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    Sampler<String> oper = new Sampler<String>();
    TestCountSink<String> sink = new TestCountSink<String>();
    oper.sample.setSink(sink);
    oper.setPassrate(10);
    oper.setTotalrate(100);

    String tuple = "a";


    int numTuples = 10000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(tuple);
    }

    oper.endWindow();
    int lowerlimit = 5;
    int upperlimit = 15;
    int actual = (100 * sink.count) / numTuples;

    Assert.assertEquals("number emitted tuples", true, lowerlimit < actual);
    Assert.assertEquals("number emitted tuples", true, upperlimit > actual);
  }
}
