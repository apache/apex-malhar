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

import com.datatorrent.lib.math.AverageMap;
import com.datatorrent.lib.math.SumCountMap;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.AverageMap}. <p>
 * Current benchmark is 12 million tuples/sec.
 *
 */
public class AverageMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(SumCountMap.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    AverageMap<String, Double> oper = new AverageMap<String, Double>();
    oper.setType(Double.class);
    CollectorTestSink averageSink = new CollectorTestSink();
    oper.average.setSink(averageSink);

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

    HashMap<String, Double> ahash = (HashMap<String, Double>)averageSink.collectedTuples.get(0);

    log.debug(String.format("\nBenchmark average for %d key/val pairs", numTuples * 3));

    log.debug(String.format("\nFor average expected(2,20,10), got(%d,%d,%d);",
                            ahash.get("a").intValue(), ahash.get("b").intValue(), ahash.get("c").intValue()));
  }
}
