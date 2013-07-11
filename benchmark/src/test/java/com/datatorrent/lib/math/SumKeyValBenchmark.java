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

import com.datatorrent.lib.math.SumKeyVal;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.SumKeyVal. }<p>
 *
 */
public class SumKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(SumKeyValBenchmark.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing() throws InterruptedException
  {
    SumKeyVal<String, Double> oper = new SumKeyVal<String, Double>();
    oper.setType(Double.class);
    CollectorTestSink sumSink = new CollectorTestSink();
    oper.sum.setSink(sumSink);

    int numTuples = 100000000;
    oper.beginWindow(0);

    for (int i = 0; i < numTuples; i++) {
      oper.data.process(new KeyValPair("a", 2.0));
      oper.data.process(new KeyValPair("b", 20.0));
      oper.data.process(new KeyValPair("c", 10.0));
    }
    oper.endWindow();

    KeyValPair<String, Double> sum1 = (KeyValPair<String, Double>)sumSink.collectedTuples.get(0);
    KeyValPair<String, Double> sum2 = (KeyValPair<String, Double>)sumSink.collectedTuples.get(1);
    KeyValPair<String, Double> sum3 = (KeyValPair<String, Double>)sumSink.collectedTuples.get(2);

    log.debug(String.format("\nBenchmark sums for %d key/val pairs", numTuples * 3));

    log.debug(String.format("\nFor sum expected (%d,%d,%d) in random order, got(%.1f,%.1f,%.1f);",
                            2 * numTuples, 20 * numTuples, 10 * numTuples,
                            sum1.getValue(), sum2.getValue(), sum3.getValue()));
  }
}
