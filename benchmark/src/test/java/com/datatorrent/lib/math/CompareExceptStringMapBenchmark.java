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

import com.datatorrent.lib.math.CompareExceptStringMap;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.CompareExcept}<p>
 *
 */
public class CompareExceptStringMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(CompareExceptStringMapBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessingSchema()
  {
    CompareExceptStringMap<String> oper = new CompareExceptStringMap<String>();
    CountAndLastTupleTestSink compareSink = new CountAndLastTupleTestSink();
    CountAndLastTupleTestSink exceptSink = new CountAndLastTupleTestSink();
    oper.compare.setSink(compareSink);
    oper.except.setSink(exceptSink);

    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    int numTuples = 50000000;
    HashMap<String, String> input1 = new HashMap<String, String>();
    HashMap<String, String> input2 = new HashMap<String, String>();

    input1.put("a", "2");
    input1.put("b", "20");
    input1.put("c", "1000");
    input2.put("a", "3");
    input2.put("b", "32");
    input2.put("c", "23");

    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmark for %d tuples", numTuples * 2));
  }
}
