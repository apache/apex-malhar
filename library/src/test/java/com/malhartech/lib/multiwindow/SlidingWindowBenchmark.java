/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.multiwindow;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.engine.TestCountAndLastTupleSink;
import com.malhartech.engine.TestCountSink;
import com.malhartech.engine.TestSink;
import com.malhartech.lib.math.Min;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.multiwindow.AbstractSlidingWindow}<p>
 *
 */
public class SlidingWindowBenchmark
{
  private static Logger log = LoggerFactory.getLogger(SlidingWindowBenchmark.class);

  public class mySlidingWindow extends AbstractSlidingWindow<String>
  {
    @OutputPortFieldAnnotation(name = "out")
    public final transient DefaultOutputPort<String> out = new DefaultOutputPort<String>(this);
    String tuplestate = "";

    @Override
    void processDataTuple(String tuple)
    {
      tuplestate = tuple;
    }

    @Override
    public void endWindow()
    {
      saveWindowState(tuplestate);
      out.emit(tuplestate);
    }

    public void dumpStates()
    {
      log.debug("\nDumping states");
      int i = getN() - 1;
      while (i >= 0) {
        Object o = getWindowState(i);
        log.debug(String.format("State %d: %s", i, (o != null) ? o.toString() : "null"));
        i--;
      }
    }
  };

  /**
   * Test functional logic
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws InterruptedException
  {
    mySlidingWindow oper = new mySlidingWindow();

    TestCountSink<String> swinSink = new TestCountSink<String>();
    oper.out.setSink(swinSink);
    oper.setN(3);
    oper.setup(null);

    int numWindows = 100000000;
    String a0 = "a0";
    String a1 = "a1";
    String a2 = "a2";
    String a3 = "a3";

    for (int i = 0; i < numWindows; i++) {
      oper.beginWindow(0);
      oper.data.process(a0);
      oper.endWindow();

      oper.beginWindow(1);
      oper.data.process(a1);
      oper.endWindow();

      oper.beginWindow(2);
      oper.data.process(a2);
      oper.endWindow();

      oper.beginWindow(3);
      oper.data.process(a3);
      oper.endWindow();
    }

    Assert.assertEquals("number emitted tuples", numWindows * 4, swinSink.getCount());
    log.debug(String.format("\nBenchmarked %d windows", numWindows * 4));
    oper.dumpStates();
  }
}
