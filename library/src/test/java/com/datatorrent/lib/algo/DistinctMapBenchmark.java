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
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.DistinctMap;
import com.datatorrent.lib.testbench.CountTestSink;

import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.DistinctMap} <p>
 *
 */
public class DistinctMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(DistinctMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings({"SleepWhileInLoop", "rawtypes", "unchecked"})
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    DistinctMap<String, Number> oper = new DistinctMap<String, Number>();

    CountTestSink sortSink = new CountTestSink<HashMap<String, Number>>();
    oper.distinct.setSink((CountTestSink<Object>)sortSink);

    HashMap<String, Number> input = new HashMap<String, Number>();

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      oper.beginWindow(0);
      input.put("a", 2);
      oper.data.process(input);
      input.clear();
      input.put("a", 2);
      oper.data.process(input);

      input.clear();
      input.put("a", 1000);
      oper.data.process(input);

      input.clear();
      input.put("a", 5);
      oper.data.process(input);

      input.clear();
      input.put("a", 2);
      input.put("b", 33);
      oper.data.process(input);

      input.clear();
      input.put("a", 33);
      input.put("b", 34);
      oper.data.process(input);

      input.clear();
      input.put("b", 34);
      oper.data.process(input);

      input.clear();
      input.put("b", 6);
      input.put("a", 2);
      oper.data.process(input);
      input.clear();
      input.put("c", 9);
      oper.data.process(input);
      oper.endWindow();
    }
    log.debug(String.format("\nBenchmarked %d tuples with %d emits", numTuples*12, sortSink.getCount()));
  }
}
