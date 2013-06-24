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

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.math.CountKeyVal;
import com.datatorrent.lib.util.KeyValPair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.CountKeyVal}<p>
 *
 */
public class CountKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(CountKeyValBenchmark.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing() throws InterruptedException
  {
    CountKeyVal<String, Double> oper = new CountKeyVal<String, Double>();
    TestSink countSink = new TestSink();
    oper.count.setSink(countSink);

    int numTuples = 100000000;
    oper.beginWindow(0);

    for (int i = 0; i < numTuples; i++) {
      oper.data.process(new KeyValPair("a", 2.0));
      oper.data.process(new KeyValPair("b", 20.0));
      oper.data.process(new KeyValPair("c", 10.0));
    }
    oper.endWindow();

    KeyValPair<String, Integer> c1 = (KeyValPair<String, Integer>)countSink.collectedTuples.get(0);
    KeyValPair<String, Integer> c2 = (KeyValPair<String, Integer>)countSink.collectedTuples.get(1);
    KeyValPair<String, Integer> c3 = (KeyValPair<String, Integer>)countSink.collectedTuples.get(2);

    log.debug(String.format("\nBenchmark sums for %d key/val pairs", numTuples * 3));

    log.debug(String.format("\nCounts were (%d,%d,%d) in random order, got(%d,%d,%d);",
                            numTuples, numTuples, numTuples,
                            c1.getValue().intValue(), c2.getValue().intValue(), c3.getValue().intValue()));
  }
}
