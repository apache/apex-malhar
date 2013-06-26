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

import com.datatorrent.api.Sink;
import com.datatorrent.lib.algo.Distinct;
import com.datatorrent.lib.testbench.CountTestSink;
import com.datatorrent.lib.testbench.HashTestSink;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.Distinct<p>
 *
 */
public class DistinctBenchmark
{
  private static Logger log = LoggerFactory.getLogger(DistinctBenchmark.class);
  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings({"SleepWhileInLoop", "rawtypes", "unchecked"})
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    Distinct<Integer> oper = new Distinct<Integer>();

    CountTestSink<Integer> sortSink = new CountTestSink<Integer>();
    Sink s = sortSink;
    oper.distinct.setSink((Sink<Object>)s);

    oper.beginWindow(0);
    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(1);
      oper.data.process(i % 13);
      oper.data.process(1);
      oper.data.process(i % 5);
      oper.data.process(1);

      oper.data.process(i % 20);
      oper.data.process(i % 2);
      oper.data.process(2);
      oper.data.process(i % 3);
      oper.data.process(1);

      oper.data.process(i % 5);
      oper.data.process(i % 10);
      oper.data.process(i % 25);
      oper.data.process(1);
      oper.data.process(3);

      oper.data.process(i % 4);
      oper.data.process(3);
      oper.data.process(1);
      oper.data.process(3);
      oper.data.process(4);
      oper.data.process(i);
    }
    oper.endWindow();

    //Assert.assertEquals("number emitted tuples", 4, sortSink.size());
    log.debug(String.format("\nBenchmarked %d tuples (emitted %d tupled)",
                            numTuples*20,
                            sortSink.getCount()));
  }
}
