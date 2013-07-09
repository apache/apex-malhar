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

import com.datatorrent.lib.math.ChangeMap;
import com.datatorrent.lib.testbench.CountTestSink;

import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.ChangeMap}. <p>
 *
 */
public class ChangeMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ChangeMapBenchmark.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeMap<String, Integer>());
    testNodeProcessingSchema(new ChangeMap<String, Double>());
    testNodeProcessingSchema(new ChangeMap<String, Float>());
    testNodeProcessingSchema(new ChangeMap<String, Short>());
    testNodeProcessingSchema(new ChangeMap<String, Long>());
  }

  public <V extends Number> void testNodeProcessingSchema(ChangeMap<String, V> oper)
  {
    CountTestSink changeSink = new CountTestSink<HashMap<String, V>>();
    CountTestSink percentSink = new CountTestSink<HashMap<String, Double>>();

    oper.change.setSink(changeSink);
    oper.percent.setSink(percentSink);

    oper.beginWindow(0);
    HashMap<String, V> input = new HashMap<String, V>();
    input.put("a", oper.getValue(2));
    input.put("b", oper.getValue(10));
    input.put("c", oper.getValue(100));
    oper.base.process(input);

    int numTuples = 1000000;

    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.put("a", oper.getValue(3));
      input.put("b", oper.getValue(2));
      input.put("c", oper.getValue(4));
      oper.data.process(input);

      input.clear();
      input.put("a", oper.getValue(4));
      input.put("b", oper.getValue(19));
      input.put("c", oper.getValue(150));
      oper.data.process(input);
    }

    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", numTuples * 6, changeSink.getCount());
    Assert.assertEquals("number emitted tuples", numTuples * 6, percentSink.getCount());
    log.debug(String.format("\nBenchmarked %d key,val pairs", numTuples * 6));
  }
}
