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

import com.datatorrent.lib.math.SquareCalculus;
import com.datatorrent.lib.testbench.CountTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.SquareCalculus}<p>
 *
 */
public class SquareCalculusBenchmark
{
  private static Logger log = LoggerFactory.getLogger(SquareCalculusBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing()
  {
    SquareCalculus oper = new SquareCalculus();
    CountTestSink lmultSink = new CountTestSink();
    CountTestSink imultSink = new CountTestSink();
    CountTestSink dmultSink = new CountTestSink();
    CountTestSink fmultSink = new CountTestSink();
    oper.longResult.setSink(lmultSink);
    oper.integerResult.setSink(imultSink);
    oper.doubleResult.setSink(dmultSink);
    oper.floatResult.setSink(fmultSink);


    oper.beginWindow(0); //
    int jtot = 100000;
    int itot = 1000;

    for (int j = 0; j < jtot; j++) {
      for (int i = 0; i < itot; i++) {
        Integer t = i;
        oper.input.process(t);
      }
    }
    oper.endWindow();
    Assert.assertEquals("number of tuples", jtot * itot, lmultSink.count);
    Assert.assertEquals("number of tuples", jtot * itot, imultSink.count);
    Assert.assertEquals("number of tuples", jtot * itot, dmultSink.count);
    Assert.assertEquals("number of tuples", jtot * itot, fmultSink.count);
  }
}
