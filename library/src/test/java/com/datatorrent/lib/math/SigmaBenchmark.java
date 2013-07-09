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

import com.datatorrent.lib.math.Sigma;
import com.datatorrent.lib.testbench.CountTestSink;

import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.Sigma}<p>
 *
 */
public class SigmaBenchmark
{
  private static Logger log = LoggerFactory.getLogger(SigmaBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing()
  {
    Sigma oper = new Sigma();
    CountTestSink lSink = new CountTestSink();
    CountTestSink iSink = new CountTestSink();
    CountTestSink dSink = new CountTestSink();
    CountTestSink fSink = new CountTestSink();
    oper.longResult.setSink(lSink);
    oper.integerResult.setSink(iSink);
    oper.doubleResult.setSink(dSink);
    oper.floatResult.setSink(fSink);

    int total = 100000000;
    ArrayList<Integer> list = new ArrayList<Integer>();
    for (int i = 0; i < 10; i++) {
      list.add(i);
    }

    for (int i = 0; i < total; i++) {
      oper.beginWindow(0); //
      oper.input.process(list);
      oper.endWindow(); //
    }
    Assert.assertEquals("sum was", total, lSink.getCount());
    Assert.assertEquals("sum was", total, iSink.getCount());
    Assert.assertEquals("sum was", total, dSink.getCount());
    Assert.assertEquals("sum was", total, fSink.getCount());

  }
}
