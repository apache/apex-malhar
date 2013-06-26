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

import com.datatorrent.lib.math.Change;
import com.datatorrent.lib.testbench.CountTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.Change}. <p>
 *
 */
public class ChangeBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ChangeBenchmark.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Change<Integer>());
    testNodeProcessingSchema(new Change<Double>());
    testNodeProcessingSchema(new Change<Float>());
    testNodeProcessingSchema(new Change<Short>());
    testNodeProcessingSchema(new Change<Long>());
  }

  /**
   *
   * @param oper
   */
  public <V extends Number> void testNodeProcessingSchema(Change<V> oper)
  {
    CountTestSink changeSink = new CountTestSink<V>();
    CountTestSink percentSink = new CountTestSink<Double>();

    oper.change.setSink(changeSink);
    oper.percent.setSink(percentSink);

    oper.beginWindow(0);
    int numTuples = 1000000;
    for (int i = 0; i < numTuples; i++) {
      oper.base.process(oper.getValue(10));
      oper.data.process(oper.getValue(5));
      oper.data.process(oper.getValue(15));
      oper.data.process(oper.getValue(20));
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", numTuples * 3, changeSink.getCount());
    Assert.assertEquals("number emitted tuples", numTuples * 3, percentSink.getCount());
    log.debug(String.format("\nBenchmarked %d key,val pairs", numTuples * 3));
  }
}
