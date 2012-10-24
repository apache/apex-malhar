/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stream.StramTestSupport;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.malhartech.lib.testbench.SeedEventClassifier} for three configuration><p>
 * <br>
 * Configuration 1: Provide values and weights<br>
 * Configuration 2: Provide values but no weights (even weights)<br>
 * Configuration 3: Provide no values or weights<br>
 * <br>
 * Benchmarks: Currently does about 3 Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 * Validates all DRC checks of the node<br>
 */
public class SeedEventClassifierTest
{
  private static Logger LOG = LoggerFactory.getLogger(EventClassifier.class);

  class TestSink implements Sink
  {
    HashMap<String, Integer> collectedTuples = new HashMap<String, Integer>();
    HashMap<String, Double> collectedTupleValues = new HashMap<String, Double>();
    boolean isstring = true;
    int count = 0;

    /**
     *
     * @param payload
     */
    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
        // LOG.debug(payload.toString());
      }
      else {
        if (isstring) {
          count++;
        }
      }
    }
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testSchemaNodeProcessing(true); // 5.9 million/sec
    testSchemaNodeProcessing(false); // 4.4 million/sec
  }

  /**
   * Test node logic emits correct results
   */
  public void testSchemaNodeProcessing(boolean isstring) throws Exception
  {
    final SeedEventClassifier node = new SeedEventClassifier();
    TestSink classifySink = new TestSink();

    Sink inSink1 = node.data1.getSink();
    Sink inSink2 = node.data2.getSink();
    if (isstring) {
      node.string_data.setSink(classifySink);
    }
    else {
      node.hash_data.setSink(classifySink);
    }

    node.setKey1("x");
    node.setKey2("y");
    node.setSeedstart(1);
    node.setSeedend(1000000);
    node.setup(new OperatorConfiguration());

    Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
    inSink1.process(bt);
    inSink2.process(bt);

    int numtuples = 50000000;
    if (isstring) {
      String input;
      for (int i = 0; i < numtuples; i++) {
        input = Integer.toString(i);
        inSink1.process(input);
        inSink2.process(input);
      }
    }
    else {
      Integer input;
      for (int i = 0; i < numtuples; i++) {
        input = new Integer(i);
        inSink1.process(input);
        inSink2.process(input);
      }
    }

    Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
    inSink1.process(et);
    inSink2.process(et);

    // Should get one bag of keys "a", "b", "c"
    try {
      for (int i = 0; i < 50; i++) {
        Thread.sleep(10);
        if (classifySink.count >= numtuples * 2 - 1) {
          break;
        }
      }
    }
    catch (InterruptedException ex) {
      LOG.debug(ex.getLocalizedMessage());
    }

    // One for each key
    Assert.assertEquals("number emitted tuples", numtuples * 2, classifySink.count);
    LOG.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", classifySink.count));
  }
}
