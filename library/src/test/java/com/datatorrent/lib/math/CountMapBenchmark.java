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

import com.datatorrent.lib.math.CountMap;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.COuntMap} <p>
 *
 */
public class CountMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(CountMapBenchmark.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing() throws InterruptedException
  {
    CountMap<String, Double> oper = new CountMap<String, Double>();
    CollectorTestSink countSink = new CollectorTestSink();
    oper.count.setSink(countSink);

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

    HashMap<String, Integer> dhash = (HashMap<String, Integer>)countSink.collectedTuples.get(0);
    log.debug(String.format("\nBenchmark sums for %d key/val pairs", numTuples * 3));

    log.debug(String.format("\nFor sum expected(%d,%d,%d), got(%d,%d,%d);",
                            2 * numTuples, 20 * numTuples, 10 * numTuples,
                            dhash.get("a").intValue(), dhash.get("b").intValue(), dhash.get("c").intValue()));
  }
}
