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
 * Functional tests for {@link org.apache.apex.malhar.lib.math.SquareCalculus}
 */
public class SquareCalculusTest
{
  /**
   * Test oper logic emits correct results
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testNodeSchemaProcessing()
  {
    SquareCalculus oper = new SquareCalculus();
    SumTestSink lmultSink = new SumTestSink();
    SumTestSink imultSink = new SumTestSink();
    SumTestSink dmultSink = new SumTestSink();
    SumTestSink fmultSink = new SumTestSink();
    oper.longResult.setSink(lmultSink);
    oper.integerResult.setSink(imultSink);
    oper.doubleResult.setSink(dmultSink);
    oper.floatResult.setSink(fmultSink);

    oper.beginWindow(0); //
    int sum = 0;
    for (int i = 0; i < 50; i++) {
      Integer t = i;
      oper.input.process(t);
      sum += i * i;
    }
    oper.endWindow(); //

    Assert.assertEquals("sum was", sum, lmultSink.val.intValue());
    Assert.assertEquals("sum was", sum, imultSink.val.intValue());
    Assert.assertEquals("sum was", sum, dmultSink.val.intValue());
    Assert.assertEquals("sum", sum, fmultSink.val.intValue());
  }
}
