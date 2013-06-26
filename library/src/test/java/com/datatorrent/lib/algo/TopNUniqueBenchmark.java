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
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.TopNUnique;
import com.datatorrent.lib.testbench.CountTestSink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.TopN}<p>
 *
 */
public class TopNUniqueBenchmark
{
  private static Logger log = LoggerFactory.getLogger(TopNUniqueBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new TopNUnique<String, Integer>());
    testNodeProcessingSchema(new TopNUnique<String, Double>());
    testNodeProcessingSchema(new TopNUnique<String, Float>());
    testNodeProcessingSchema(new TopNUnique<String, Short>());
    testNodeProcessingSchema(new TopNUnique<String, Long>());
  }

  public void testNodeProcessingSchema(TopNUnique oper)
  {
    CountTestSink<HashMap<String, Number>> sortSink = new CountTestSink<HashMap<String, Number>>();
    oper.top.setSink(sortSink);
    oper.setN(3);

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();

    int numTuples = 5000000;
    for (int j = 0; j < numTuples / 1000; j++) {
      for (int i = 999; i >= 0; i--) {
        input.put("a", i);
        input.put("b", numTuples - i);
        oper.data.process(input);
      }
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 2, sortSink.getCount());
    log.debug(String.format("\nBenchmaked %d tuples", numTuples * 2));
  }
}
