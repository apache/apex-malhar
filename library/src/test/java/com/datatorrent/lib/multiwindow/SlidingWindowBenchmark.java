/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.multiwindow;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.math.MinMap;
import com.datatorrent.lib.multiwindow.AbstractSlidingWindow;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import com.datatorrent.lib.testbench.CountTestSink;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.multiwindow.AbstractSlidingWindow}<p>
 *
 */
public class SlidingWindowBenchmark
{
  private static Logger log = LoggerFactory.getLogger(SlidingWindowBenchmark.class);

  public class mySlidingWindow extends AbstractSlidingWindow<String>
  {
    @OutputPortFieldAnnotation(name = "out")
    public final transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();
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
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws InterruptedException
  {
    mySlidingWindow oper = new mySlidingWindow();

    CountTestSink swinSink = new CountTestSink<String>();
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
