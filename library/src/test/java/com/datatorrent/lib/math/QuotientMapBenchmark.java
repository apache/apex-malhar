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

import com.datatorrent.lib.math.QuotientMap;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.QuotientMap}<p>
 *
 */
public class QuotientMapBenchmark
{
  private static Logger LOG = LoggerFactory.getLogger(QuotientMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new QuotientMap<String, Integer>());
    testNodeProcessingSchema(new QuotientMap<String, Double>());
  }

  public void testNodeProcessingSchema(QuotientMap oper) throws Exception
  {

    CountAndLastTupleTestSink quotientSink = new CountAndLastTupleTestSink();

    oper.quotient.setSink(quotientSink);
    oper.setMult_by(2);

    oper.beginWindow(0); //
    HashMap<String, Number> input = new HashMap<String, Number>();
    int numtuples = 100000000;
    for (int i = 0; i < numtuples; i++) {
      input.clear();
      input.put("a", 2);
      input.put("b", 20);
      input.put("c", 1000);
      oper.numerator.process(input);
      input.clear();
      input.put("a", 2);
      input.put("b", 40);
      input.put("c", 500);
      oper.denominator.process(input);
    }

    oper.endWindow();
    // One for each key
    LOG.debug(String.format("Processed %d tuples", numtuples * 6));

    HashMap<String, Number> output = (HashMap<String, Number>)quotientSink.tuple;
    for (Map.Entry<String, Number> e: output.entrySet()) {
      LOG.debug(String.format("Key, value is %s,%f", e.getKey(), e.getValue().doubleValue()));
    }
  }
}
