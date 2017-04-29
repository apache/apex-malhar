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
package org.apache.apex.malhar.lib.window.accumulation;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.SumAccumulation;
import org.apache.apex.malhar.lib.window.accumulation.CompositeAccumulation.AccumulationTag;

public class CompositeAccumulationTest
{
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testIncremental()
  {
    CompositeAccumulation<Long> accumulations = new CompositeAccumulation<>();
    AccumulationTag sumTag = accumulations.addAccumulation((Accumulation)new SumAccumulation());
    AccumulationTag countTag = accumulations.addAccumulation((Accumulation)new Count());
    AccumulationTag maxTag = accumulations.addAccumulation(new Max());
    AccumulationTag minTag = accumulations.addAccumulation(new Min());
    List values = accumulations.defaultAccumulatedValue();
    for (long i = 1; i <= 10; i++) {
      values = accumulations.accumulate(values, i);
    }

    List outputValues = accumulations.getOutput(values);
    Assert.assertTrue((Long)accumulations.getSubOutput(sumTag, outputValues) == 55L);
    Assert.assertTrue((Long)accumulations.getSubOutput(countTag, outputValues) == 10L);
    Assert.assertTrue((Long)accumulations.getSubOutput(maxTag, outputValues) == 10L);
    Assert.assertTrue((Long)accumulations.getSubOutput(minTag, outputValues) == 1L);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testAverage()
  {
    CompositeAccumulation<Double> accumulations = new CompositeAccumulation<>();
    AccumulationTag averageTag = accumulations.addAccumulation((Accumulation)new Average());
    List values = accumulations.defaultAccumulatedValue();
    for (int i = 1; i <= 10; i++) {
      values = accumulations.accumulate(values, i * 1.0);
    }

    List outputValues = accumulations.getOutput(values);
    Assert.assertTrue((Double)accumulations.getSubOutput(averageTag, outputValues) == 5.5);
  }
}
