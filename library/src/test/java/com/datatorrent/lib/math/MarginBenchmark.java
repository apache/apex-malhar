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

import com.datatorrent.lib.math.Margin;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.Margin}<p>
 *
 */
public class MarginBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MarginBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Margin<Double>());
  }

  public void testNodeProcessingSchema(Margin oper)
  {
    CountAndLastTupleTestSink marginSink = new CountAndLastTupleTestSink();

    oper.margin.setSink(marginSink);
    oper.setPercent(true);
    oper.beginWindow(0);

    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
      oper.numerator.process(2);
      oper.numerator.process(20);
      oper.numerator.process(1000);
      oper.denominator.process(4);
      oper.denominator.process(40);
      oper.denominator.process(2000);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 6));
    Assert.assertEquals("number emitted tuples", 1, marginSink.count);
    Assert.assertEquals("margin was ", 50, ((Number)marginSink.tuple).intValue());
  }
}
