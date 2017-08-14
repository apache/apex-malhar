/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.math;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.SumTestSink;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.math.MultiplyByConstant}
 */

public class MultiplyByConstantTest
{
  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeSchemaProcessing()
  {
    MultiplyByConstant oper = new MultiplyByConstant();
    SumTestSink<Object> lmultSink = new SumTestSink<Object>();
    SumTestSink<Object> imultSink = new SumTestSink<Object>();
    SumTestSink<Object> dmultSink = new SumTestSink<Object>();
    SumTestSink<Object> fmultSink = new SumTestSink<Object>();
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
