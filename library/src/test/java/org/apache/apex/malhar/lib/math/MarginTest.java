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

import org.apache.apex.malhar.lib.testbench.CountAndLastTupleTestSink;


/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.math.Margin}<p>
 *
 */
public class MarginTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Margin<Integer>());
    testNodeProcessingSchema(new Margin<Double>());
    testNodeProcessingSchema(new Margin<Float>());
    testNodeProcessingSchema(new Margin<Short>());
    testNodeProcessingSchema(new Margin<Long>());
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testNodeProcessingSchema(Margin oper)
  {
    CountAndLastTupleTestSink marginSink = new CountAndLastTupleTestSink();

    oper.margin.setSink(marginSink);
    oper.setPercent(true);

    oper.beginWindow(0);
    oper.numerator.process(2);
    oper.numerator.process(20);
    oper.numerator.process(100);
    oper.denominator.process(200);
    oper.denominator.process(22);
    oper.denominator.process(22);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, marginSink.count);
    Assert.assertEquals("margin was ", 50, ((Number)marginSink.tuple).intValue());

    marginSink.clear();
    oper.beginWindow(0);
    oper.numerator.process(2);
    oper.numerator.process(20);
    oper.numerator.process(100);
    oper.denominator.process(17);
    oper.denominator.process(22);
    oper.denominator.process(22);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, marginSink.count);
    Assert.assertEquals("margin was ", -100, ((Number)marginSink.tuple).intValue());

    marginSink.clear();
    oper.beginWindow(0);
    oper.numerator.process(2);
    oper.numerator.process(20);
    oper.numerator.process(100);
    oper.denominator.process(17);
    oper.denominator.process(22);
    oper.denominator.process(-39);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 0, marginSink.count);
  }
}
