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
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.FilterValues;
import com.datatorrent.lib.testbench.CountTestSink;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.FilterValues}<p>
 *
 */
public class FilterValuesBenchmark
{
  private static Logger log = LoggerFactory.getLogger(FilterValuesBenchmark.class);

  int getTotal(List list)
  {
    ArrayList<Integer> ilist = (ArrayList<Integer>)list;
    int ret = 0;
    for (Integer i: ilist) {
      ret += i.intValue();
    }
    return ret;
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings( {"SleepWhileInLoop", "unchecked"})
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    FilterValues<Integer> oper = new FilterValues<Integer>();

    CountTestSink sortSink = new CountTestSink<Integer>();
    oper.filter.setSink((CountTestSink<Object>)sortSink);
    Integer [] values = new Integer[2];
    oper.setValue(5);
    oper.clearValues();
    values[0] = 200;
    values[1] = 2;
    oper.setValue(4);
    oper.setValues(values);

    oper.beginWindow(0);

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      oper.setInverse(false);
      oper.data.process(2);
      oper.data.process(5);
      oper.data.process(7);
      oper.data.process(42);
      oper.data.process(200);
      oper.data.process(2);
      oper.data.process(2);
      oper.data.process(33);
      oper.data.process(2);
      oper.data.process(6);
      oper.data.process(2);
      oper.data.process(6);
      oper.data.process(2);
      oper.data.process(6);
      oper.data.process(2);
      oper.data.process(6);
      oper.data.process(2);
      oper.setInverse(true);
      oper.data.process(9);
    }

    log.debug(String.format("\nBenchmarked %d tuples, and emitted %d tuples", numTuples * 17, sortSink.getCount()));
    oper.endWindow();
  }
}
