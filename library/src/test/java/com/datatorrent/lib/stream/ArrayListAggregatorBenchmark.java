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
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.ArrayListAggregator;
import com.datatorrent.lib.testbench.CountTestSink;

import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.datatorrent.lib.testbench.ArrayListAggregator}<p>
 */
public class ArrayListAggregatorBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ArrayListAggregatorBenchmark.class);

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    ArrayListAggregator<Integer> oper = new ArrayListAggregator<Integer>();
    CountTestSink cSink = new CountTestSink();

    oper.output.setSink(cSink);
    int size = 10;
    oper.setSize(size);
    int numtuples = 100000000;

    oper.beginWindow(0);
    for (int i = 0; i < numtuples; i++) {
      oper.input.process(i);
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", numtuples/size, cSink.getCount());
    log.debug(String.format("\nProcessed %d tuples", numtuples));
  }
}
