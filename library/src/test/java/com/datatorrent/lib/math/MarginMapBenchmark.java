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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.MarginMap;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.MarginMap}<p>
 *
 */
public class MarginMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MarginMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MarginMap<String, Integer>());
  }

  public void testNodeProcessingSchema(MarginMap oper)
  {
    CountAndLastTupleTestSink marginSink = new CountAndLastTupleTestSink();

    oper.margin.setSink(marginSink);
    oper.beginWindow(0);
    HashMap<String, Number> ninput = new HashMap<String, Number>();
    HashMap<String, Number> dinput = new HashMap<String, Number>();

    int numTuples = 100000000;
    ninput.put("a", 2);
    ninput.put("b", 20);
    ninput.put("c", 1000);
    dinput.put("a", 2);
    dinput.put("b", 40);
    dinput.put("c", 500);

    for (int i = 0; i < numTuples; i++) {
      oper.numerator.process(ninput);
      oper.denominator.process(dinput);
    }

    oper.endWindow();
    log.debug(String.format("number emitted tuples are %d", numTuples * 6));
    HashMap<String, Number> output = (HashMap<String, Number>)marginSink.tuple;
    for (Map.Entry<String, Number> e: output.entrySet()) {
      log.debug(String.format("Key, value is %s,%f", e.getKey(), e.getValue().doubleValue()));
    }
  }
}
