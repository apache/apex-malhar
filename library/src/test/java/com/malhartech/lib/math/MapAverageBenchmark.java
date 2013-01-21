/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.malhartech.lib.math.MapAverage}. <p>
 * Current benchmark is 12 million tuples/sec.
 *
 */
public class MapAverageBenchmark
{
  private static Logger log = LoggerFactory.getLogger(Sum.class);

  class TestSink implements Sink
  {
    List<Object> collectedTuples = new ArrayList<Object>();

    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        collectedTuples.add(payload);
      }
    }
  }

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    MapAverage<String, Double> oper = new MapAverage<String, Double>();
    oper.setType(Double.class);
    com.malhartech.engine.TestSink averageSink = new com.malhartech.engine.TestSink();
    oper.average.setSink(averageSink);

    int numTuples = 100000000;
    oper.beginWindow(0);
    HashMap<String, Double> input = new HashMap<String, Double>();

    for (int i = 0; i < numTuples; i++) {
      input.put("a", 2.0);
      input.put("b", 20.0);
      input.put("c", 10.0);
      oper.data.process(input);
    }
    oper.endWindow();

    HashMap<String, Double> ahash = (HashMap<String, Double>)averageSink.collectedTuples.get(0);

    log.debug(String.format("\nBenchmark average for %d key/val pairs", numTuples * 3));

    log.debug(String.format("\nFor average expected(2,20,10), got(%d,%d,%d);",
                            ahash.get("a").intValue(), ahash.get("b").intValue(), ahash.get("c").intValue()));
  }
}
