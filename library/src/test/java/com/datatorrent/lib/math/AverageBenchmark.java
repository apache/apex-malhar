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
 * limitations under the License.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.Average;
import com.datatorrent.lib.testbench.CollectorTestSink;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.Average}. <p>
 * Current benchmark is 175 million tuples per second.
 *
 */
public class AverageBenchmark
{
  private static Logger log = LoggerFactory.getLogger(AverageBenchmark.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    Average<Double> doper = new Average<Double>();
    Average<Float> foper = new Average<Float>();
    Average<Integer> ioper = new Average<Integer>();
    Average<Long> loper = new Average<Long>();
    Average<Short> soper = new Average<Short>();
    doper.setType(Double.class);
    foper.setType(Float.class);
    ioper.setType(Integer.class);
    loper.setType(Long.class);
    soper.setType(Short.class);

    testNodeSchemaProcessing(doper, "Double");
    testNodeSchemaProcessing(foper, "Float");
    testNodeSchemaProcessing(ioper, "Integer");
    testNodeSchemaProcessing(loper, "Long");
    testNodeSchemaProcessing(soper, "Short");
  }

  public void testNodeSchemaProcessing(Average oper, String debug)
  {
    CollectorTestSink averageSink = new CollectorTestSink();
    oper.average.setSink(averageSink);

    oper.beginWindow(0); //

    Double a = new Double(2.0);
    Double b = new Double(2.0);
    Double c = new Double(1.0);

    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(a);
      oper.data.process(b);
      oper.data.process(c);
    }
    oper.endWindow(); //

    Number average = (Number)averageSink.collectedTuples.get(0);
    log.debug(String.format("\nBenchmark %d tuples of type %s: taverage was %f",
                            numTuples * 3, debug, average.doubleValue()));
  }
}
