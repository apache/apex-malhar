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

import com.datatorrent.lib.algo.FirstMatchStringMap;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.FirstMatchStringMap}<p>
 *
 */
public class FirstMatchStringBenchmark
{
  private static Logger log = LoggerFactory.getLogger(FirstMatchStringBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    FirstMatchStringMap<String> oper = new FirstMatchStringMap<String>();
    CountAndLastTupleTestSink matchSink = new CountAndLastTupleTestSink();
    oper.first.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3);
    oper.setTypeEQ();

    HashMap<String, String> input = new HashMap<String, String>();

    oper.beginWindow(0);
    matchSink.clear();

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      input.put("a", "4");
      input.put("b", "20");
      input.put("c", "1000");
      oper.data.process(input);
      input.put("a", "3");
      input.put("b", "20");
      input.put("c", "1000");
      oper.data.process(input);
      input.clear();
      input.put("a", "2");
      oper.data.process(input);
      input.clear();
      input.put("a", "4");
      input.put("b", "21");
      input.put("c", "1000");
      oper.data.process(input);
      input.clear();
      input.put("a", "4");
      input.put("b", "20");
      input.put("c", "5");
      oper.data.process(input);
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, matchSink.count);
    HashMap<String, String> tuple = (HashMap<String, String>) matchSink.tuple;
    String aval = tuple.get("a");
    Assert.assertEquals("Value of a was ", "3", aval);

    oper.beginWindow(0);
    matchSink.clear();
    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.put("a", "2");
      input.put("b", "20");
      input.put("c", "1000");
      oper.data.process(input);
      input.clear();
      input.put("a", "5");
      oper.data.process(input);
    }
    oper.endWindow();
    // There should be no emit as all tuples do not match
    Assert.assertEquals("number emitted tuples", 0, matchSink.count);
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 17));
  }
}
