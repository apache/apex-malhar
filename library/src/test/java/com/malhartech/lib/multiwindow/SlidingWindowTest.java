/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.multiwindow;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.engine.TestCountAndLastTupleSink;
import com.malhartech.engine.TestSink;
import com.malhartech.lib.math.Min;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.multiwindow.AbstractSlidingWindow}<p>
 *
 */
public class SlidingWindowTest
{
  private static Logger log = LoggerFactory.getLogger(SlidingWindowTest.class);

  public class mySlidingWindow extends AbstractSlidingWindow<String>
  {
    @OutputPortFieldAnnotation(name = "out")
    public final transient DefaultOutputPort<ArrayList<String>> out = new DefaultOutputPort<ArrayList<String>>(this);

    ArrayList<String> tuples = new ArrayList<String>();

    @Override
    void processDataTuple(String tuple)
    {
      tuples.add(tuple);
    }

    @Override
    public void endWindow()
    {
      saveWindowState(tuples);
      out.emit(tuples);
      tuples = new ArrayList<String>();
    }

    public void dumpStates()
    {
      log.debug("\nDumping states");
      int i = getN()-1;
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
  public void testNodeProcessing() throws InterruptedException
  {
    mySlidingWindow oper = new mySlidingWindow();

    TestSink<ArrayList<String>> swinSink = new TestSink<ArrayList<String>>();
    oper.out.setSink(swinSink);
    oper.setN(3);
    oper.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));

    oper.beginWindow(0);
    oper.data.process("a0");
    oper.data.process("b0");
    oper.endWindow();

    oper.beginWindow(1);
    oper.data.process("a1");
    oper.data.process("b1");
    oper.endWindow();

    oper.beginWindow(2);
    oper.data.process("a2");
    oper.data.process("b2");
    oper.endWindow();

    oper.beginWindow(3);
    oper.data.process("a3");
    oper.data.process("b3");
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 4, swinSink.collectedTuples.size());
    for (Object o : swinSink.collectedTuples) {
      log.debug(o.toString());
    }
    oper.dumpStates();
   }
}
