/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Context;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
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
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Quotient<String,Integer>());
    testNodeProcessingSchema(new Quotient<String,Double>());
  }

  public void testNodeProcessingSchema(Quotient node) throws Exception
  {

    TestSink quotientSink = new TestSink();
    Sink numSink = node.numerator.getSink();
    Sink denSink = node.denominator.getSink();
    node.quotient.setSink(quotientSink);
    node.setup(new OperatorConfiguration());
    node.setMult_by(2);

    Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
    numSink.process(bt);
    denSink.process(bt);

    HashMap<String,Number> ninput = new HashMap<String, Number>();
    HashMap<String,Number> dinput = new HashMap<String, Number>();
    int numtuples = 10000000;
    for (int i = 0; i < numtuples; i++) {
      ninput = new HashMap<String, Number>();
      dinput = new HashMap<String, Number>();
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
