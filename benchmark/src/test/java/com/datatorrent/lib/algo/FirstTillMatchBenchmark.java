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

import com.datatorrent.lib.algo.FirstTillMatch;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.FirstTillMatch}<p>
 *
 */
public class FirstTillMatchBenchmark
{
  private static Logger log = LoggerFactory.getLogger(FirstTillMatchBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new FirstTillMatch<String, Integer>());
    testNodeProcessingSchema(new FirstTillMatch<String, Double>());
    testNodeProcessingSchema(new FirstTillMatch<String, Float>());
    testNodeProcessingSchema(new FirstTillMatch<String, Short>());
    testNodeProcessingSchema(new FirstTillMatch<String, Long>());
  }

  @SuppressWarnings("unchecked")
  public void testNodeProcessingSchema(FirstTillMatch oper)
  {
    CollectorTestSink matchSink = new CollectorTestSink();
    oper.first.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3);
    oper.setTypeEQ();

    int numTuples = 1000000;
    for (int i = 0; i < numTuples; i++) {
      matchSink.clear();
      oper.beginWindow(0);
      HashMap<String, Number> input = new HashMap<String, Number>();
      input.put("a", 4);
      input.put("b", 20);
      input.put("c", 1000);
      oper.data.process(input);
      input.clear();
      input.put("a", 2);
      oper.data.process(input);
      input.put("a", 3);
      input.put("b", 20);
      input.put("c", 1000);
      oper.data.process(input);
      input.clear();
      input.put("a", 4);
      input.put("b", 21);
      input.put("c", 1000);
      oper.data.process(input);
      input.clear();
      input.put("a", 6);
      input.put("b", 20);
      input.put("c", 5);
      oper.data.process(input);
      oper.endWindow();
    }

    Assert.assertEquals("number emitted tuples", 2, matchSink.collectedTuples.size());
    int atotal = 0;
    Number aval;
    for (Object o: matchSink.collectedTuples) {
      aval = ((HashMap<String, Number>)o).get("a");
      atotal += aval.intValue();
    }
    Assert.assertEquals("Value of a was ", 6, atotal);
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 13));
  }

}
