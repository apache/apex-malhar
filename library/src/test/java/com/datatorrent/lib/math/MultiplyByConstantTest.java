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

import com.datatorrent.lib.math.MultiplyByConstant;
import com.datatorrent.lib.testbench.SumTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.MultiplyByConstact}<p>
 *
 */
public class MultiplyByConstantTest
{
  private static Logger log = LoggerFactory.getLogger(MultiplyByConstantTest.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeSchemaProcessing()
  {
    MultiplyByConstant oper = new MultiplyByConstant();
    SumTestSink lmultSink = new SumTestSink();
    SumTestSink imultSink = new SumTestSink();
    SumTestSink dmultSink = new SumTestSink();
    SumTestSink fmultSink = new SumTestSink();
    oper.longProduct.setSink(lmultSink);
    oper.integerProduct.setSink(imultSink);
    oper.doubleProduct.setSink(dmultSink);
    oper.floatProduct.setSink(fmultSink);
    int imby = 2;
    Integer mby = imby;
    oper.setMultiplier(mby);

    oper.beginWindow(0); //

    int sum = 0;
    for (int i = 0; i < 100; i++) {
      Integer t = i;
      oper.input.process(t);
      sum += i * imby;
    }

    oper.endWindow(); //

    Assert.assertEquals("sum was", sum, lmultSink.val.intValue());
    Assert.assertEquals("sum was", sum, imultSink.val.intValue());
    Assert.assertEquals("sum was", sum, dmultSink.val.intValue());
    Assert.assertEquals("sum", sum, fmultSink.val.intValue());
  }
}
