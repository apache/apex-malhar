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

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.math.AverageKeyVal;
import com.datatorrent.lib.util.KeyValPair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.AverageKeyVal}. <p>
 * Current benchmark 13 million tuples per second.
 *
 */
public class AverageKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(AverageKeyValBenchmark.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing() throws InterruptedException
  {
    AverageKeyVal<String> oper = new AverageKeyVal<String>();
    TestSink averageSink = new TestSink();
    oper.doubleAverage.setSink(averageSink);

    int numTuples = 100000000;
    oper.beginWindow(0);

    for (int i = 0; i < numTuples; i++) {
      oper.data.process(new KeyValPair("a", 2.0));
      oper.data.process(new KeyValPair("b", 20.0));
      oper.data.process(new KeyValPair("c", 10.0));
    }
    oper.endWindow();

    KeyValPair<String, Double> ave1 = (KeyValPair<String, Double>)averageSink.collectedTuples.get(0);
    KeyValPair<String, Double> ave2 = (KeyValPair<String, Double>)averageSink.collectedTuples.get(1);
    KeyValPair<String, Double> ave3 = (KeyValPair<String, Double>)averageSink.collectedTuples.get(2);

    log.debug(String.format("\nBenchmark sums for %d key/val pairs", numTuples * 3));

    log.debug(String.format("\nFor average expected(2,20,10) in random order, got(%d,%d,%d);",
                            ave1.getValue().intValue(), ave2.getValue().intValue(), ave3.getValue().intValue()));
  }
}
