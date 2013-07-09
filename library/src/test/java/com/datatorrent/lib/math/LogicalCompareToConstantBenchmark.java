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

/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
import com.datatorrent.lib.math.LogicalCompareToConstant;
import com.datatorrent.lib.testbench.CountTestSink;
import com.datatorrent.common.util.Pair;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.LogicalCompareToConstant}<p>
 *
 */
public class LogicalCompareToConstantBenchmark
{
  /**
   * Test operator logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    LogicalCompareToConstant<Integer> oper = new LogicalCompareToConstant<Integer>()
    {
    };
    CountTestSink eSink = new CountTestSink();
    CountTestSink neSink = new CountTestSink();
    CountTestSink gtSink = new CountTestSink();
    CountTestSink gteSink = new CountTestSink();
    CountTestSink ltSink = new CountTestSink();
    CountTestSink lteSink = new CountTestSink();

    oper.equalTo.setSink(eSink);
    oper.notEqualTo.setSink(neSink);
    oper.greaterThan.setSink(gtSink);
    oper.greaterThanOrEqualTo.setSink(gteSink);
    oper.lessThan.setSink(ltSink);
    oper.lessThanOrEqualTo.setSink(lteSink);
    oper.setConstant(2);


    int itot = 100000000;
    oper.beginWindow(0); //

    for (int i = 0; i < itot; i++) {
      oper.input.process(1);
      oper.input.process(2);
      oper.input.process(3);
    }
    oper.endWindow(); //

    Assert.assertEquals("number emitted tuples", itot, eSink.getCount());
    Assert.assertEquals("number emitted tuples", 2 * itot, neSink.getCount());
    Assert.assertEquals("number emitted tuples", itot, gtSink.getCount());
    Assert.assertEquals("number emitted tuples", 2 * itot, gteSink.getCount());
    Assert.assertEquals("number emitted tuples", itot, ltSink.getCount());
    Assert.assertEquals("number emitted tuples", 2 * itot, lteSink.getCount());
  }
}
