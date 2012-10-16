/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.ModuleContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.TestSink;
import com.malhartech.dag.Tuple;
import com.malhartech.stream.StramTestSupport;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class QuotientTest
{
  private static Logger LOG = LoggerFactory.getLogger(Quotient.class);

  /**
   * Test configuration and parameter validation of the node
   */
  @Test
  public void testNodeValidation()
  {
    Quotient node = new Quotient();

    ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
    conf.set(Quotient.KEY_MULTIPLY_BY, "junk");

    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + Quotient.KEY_MULTIPLY_BY);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + Quotient.KEY_MULTIPLY_BY,
                        e.getMessage().contains("has to be an an integer"));
    }
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    final Quotient node = new Quotient();

    TestSink quotientSink = new TestSink();

    Sink numSink = node.connect(Quotient.IPORT_NUMERATOR, node);
    Sink denSink = node.connect(Quotient.IPORT_DENOMINATOR, node);
    node.connect(Quotient.OPORT_QUOTIENT, quotientSink);

    ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
    conf.setInt(Quotient.KEY_MULTIPLY_BY, 2);
    node.setup(conf);

    final AtomicBoolean inactive = new AtomicBoolean(true);
    new Thread()
    {
      @Override
      public void run()
      {
        inactive.set(false);
        node.activate(new ModuleContext("ArithmeticQuotientTestNode", this));
      }
    }.start();

    /**
     * spin while the node gets activated.
     */
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

    Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
    numSink.process(bt);
    denSink.process(bt);

    HashMap<String, Integer> ninput = null;
    HashMap<String, Integer> dinput = null;

    int numtuples = 10000000;
    for (int i = 0; i < numtuples; i++) {
      ninput = new HashMap<String, Integer>();
      dinput = new HashMap<String, Integer>();
      ninput.put("a", 2);
      ninput.put("b", 20);
      ninput.put("c", 1000);
      numSink.process(ninput);
      dinput.put("a", 2);
      dinput.put("b", 40);
      dinput.put("c", 500);
      denSink.process(dinput);
    }

    Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
    numSink.process(et);
    denSink.process(et);

    // Should get one bag of keys "a", "b", "c"
    try {
      for (int i = 0; i < 50; i++) {
        Thread.sleep(20);
        if (quotientSink.collectedTuples.size() >= 1) {
          break;
        }
      }
    }
    catch (InterruptedException ex) {
      LOG.debug(ex.getLocalizedMessage());
    }

    // One for each key
    Assert.assertEquals("number emitted tuples", 1, quotientSink.collectedTuples.size());
    LOG.debug(String.format("Processed %d tuples", numtuples * 6));

    for (Object o: quotientSink.collectedTuples) {
      HashMap<String, Number> output = (HashMap<String, Number>)o;
      for (Map.Entry<String, Number> e: output.entrySet()) {
        LOG.debug(String.format("Key, value is %s,%f", e.getKey(), e.getValue().doubleValue()));
        if (e.getKey().equals("a")) {
          Assert.assertEquals("emitted value for 'a' was ", new Double(2), e.getValue());
        }
        else if (e.getKey().equals("b")) {
          Assert.assertEquals("emitted tuple for 'b' was ", new Double(1), e.getValue());
        }
        else if (e.getKey().equals("c")) {
          Assert.assertEquals("emitted tuple for 'c' was ", new Double(4), e.getValue());
        }
        else {
          LOG.debug(String.format("key was %s", e.getKey()));
        }
      }
    }
  }
}
