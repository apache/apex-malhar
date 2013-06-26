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

import com.datatorrent.lib.math.Max;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.Max}. <p>
 *
 */
public class MaxBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MaxBenchmark.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    Max<Double> oper = new Max<Double>();
    CountAndLastTupleTestSink maxSink = new CountAndLastTupleTestSink();

    oper.max.setSink(maxSink);

    // Not needed, but still setup is being called as a matter of discipline
    oper.beginWindow(0); //

    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
      Double a = new Double(2.0);
      Double b = new Double(20.0);
      Double c = new Double(1000.0);
      oper.data.process(a);
      oper.data.process(b);
      oper.data.process(c);
      a = 1.0;
      oper.data.process(a);
      a = 10.0;
      oper.data.process(a);
      b = 5.0;
      oper.data.process(b);
      b = 12.0;
      oper.data.process(b);
      c = 22.0;
      oper.data.process(c);
      c = 14.0;
      oper.data.process(c);
      a = 46.0;
      oper.data.process(a);
      b = 2.0;
      oper.data.process(b);
      a = 23.0;
      oper.data.process(a);
    }

    oper.endWindow(); //
    log.debug(String.format("\nBenchmark for %d tuples; expected 1.0, got %f from %d tuples", numTuples * 12,
                            maxSink.tuple, maxSink.count));
  }
}
