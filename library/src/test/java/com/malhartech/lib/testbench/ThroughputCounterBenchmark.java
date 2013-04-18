/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;


import com.malhartech.api.Sink;
import com.malhartech.engine.OperatorContext;
import com.malhartech.tuple.Tuple;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.testbench.ThroughputCounter}. <p>
 * <br>
 * Load is generated and the tuples are outputted to ensure that the numbers are roughly in line with the weights<br>
 * <br>
 *  Benchmarks:<br>
 * String schema generates over 11 Million tuples/sec<br>
 * HashMap schema generates over 1.7 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 *
 */
public class ThroughputCounterBenchmark {

    private static Logger log = LoggerFactory.getLogger(ThroughputCounterBenchmark.class);

  class TestCountSink implements Sink
  {
    long count = 0;
    long average = 0;

    /**
     * @param payload
     */
    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
        // LOG.debug(payload.toString());
      }
      else {
        HashMap<String, Number> tuples = (HashMap<String, Number>)payload;
        average = ((Long) tuples.get(ThroughputCounter.OPORT_COUNT_TUPLE_AVERAGE)).longValue();
        count += ((Long) tuples.get(ThroughputCounter.OPORT_COUNT_TUPLE_COUNT)).longValue();
      }
    }
  }

  /**
   * Tests both string and non string schema
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testSingleSchemaNodeProcessing() throws Exception
  {
    ThroughputCounter<String, Integer> node = new ThroughputCounter<String, Integer>();

    TestCountSink countSink = new TestCountSink();
    node.count.setSink(countSink);
    node.setRollingWindowCount(5);
    node.setup(null);

    node.beginWindow(0);
    HashMap<String, Integer> input;
    int aint = 100;
    int bint = 10;
    Integer aval = new Integer(aint);
    Integer bval = new Integer(bint);
    int numtuples = 100000000;
    int sentval = 0;
    for (int i = 0; i < numtuples; i++) {
      input = new HashMap<String, Integer>();
      input.put("a", aval);
      input.put("b", bval);
      sentval += 2;
      node.data.process(input);
    }
    node.endWindow();

    log.info(String.format("\n*******************************************************\nGot average per sec(%d), count(got %d, expected %d), numtuples(%d)",
                           countSink.average,
                           countSink.count,
                           (aint+bint) * numtuples,
                           sentval));
  }
}
