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
package com.datatorrent.lib.multiwindow;

import com.datatorrent.lib.multiwindow.SimpleMovingAverage;
import com.datatorrent.lib.testbench.CountTestSink;
import com.datatorrent.lib.util.KeyValPair;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.datatorrent.lib.multiwindow.SimpleMovingAverage}. <p>
 *
 */
public class SimpleMovingAverageBenchmark
{
  private static final Logger logger = LoggerFactory.getLogger(SimpleMovingAverageBenchmark.class);

  /**
   * Test functional logic.
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws InterruptedException
  {
    SimpleMovingAverage<String, Double> oper = new SimpleMovingAverage<String, Double>();

    CountTestSink sink = new CountTestSink<KeyValPair<String, Double>>();
    oper.doubleSMA.setSink(sink);
    oper.setWindowSize(3);

    long numTuples = 1000000;
    for (int i = 0; i < numTuples; ++i) {
      double val = 30;
      double val2 = 51;
      oper.beginWindow(0);
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.endWindow();

      oper.beginWindow(1);
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.endWindow();

      oper.beginWindow(2);
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.endWindow();

      oper.beginWindow(3);
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("a", ++val));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.data.process(new KeyValPair<String, Double>("b", ++val2));
      oper.endWindow();
    }

    Assert.assertEquals("number emitted tuples", numTuples * 8, sink.count);
    logger.debug(String.format("\nBenchmarked tuple count %d", numTuples * 8));

  }
}
