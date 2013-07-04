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

import com.datatorrent.lib.algo.MergeSort;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.MergeSortDesc}<p>
 */
public class MergeSortDescBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MergeSortDescBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MergeSort<Integer>(), "Integer");
    testNodeProcessingSchema(new MergeSort<Double>(), "Double");
    testNodeProcessingSchema(new MergeSort<Float>(), "Float");
    testNodeProcessingSchema(new MergeSort<String>(), "String");
  }

  public void testNodeProcessingSchema(MergeSort oper, String debug)
  {
    //FirstN<String,Float> aoper = new FirstN<String,Float>();
    CollectorTestSink sortSink = new CollectorTestSink();
    oper.sort.setSink(sortSink);
    ArrayList input = new ArrayList();

    int numTuples = 1000000;
    oper.beginWindow(0);

    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.add(numTuples-i);
      oper.data.process(input);

      input.clear();
      input.add(2);
      oper.data.process(input);

      input.clear();
      input.add(20);
      oper.data.process(input);

      input.clear();
      input.add(1000);
      input.add(5);
      input.add(20);
      input.add(33);
      input.add(33);
      input.add(34);
      oper.data.process(input);

      input.clear();
      input.add(34);
      input.add(1001);
      input.add(6);
      input.add(1);
      input.add(9);
      oper.data.process(input);
    }

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    ArrayList list = (ArrayList) sortSink.collectedTuples.get(0);
    log.debug(String.format("Benchmarked %d tuples (%d uniques) of type %s\n", numTuples * 4, list.size(), debug));
  }
}
