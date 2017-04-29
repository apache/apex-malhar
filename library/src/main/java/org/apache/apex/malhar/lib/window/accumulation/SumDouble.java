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

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.commons.lang3.mutable.MutableDouble;

/**
 * Sum Accumulation for doubles.
 *
 * @since 3.5.0
 */
public class SumDouble implements Accumulation<Double, MutableDouble, Double>
{
  @Override
  public MutableDouble defaultAccumulatedValue()
  {
    return new MutableDouble(0.0);
  }

  @Override
  public MutableDouble accumulate(MutableDouble accumulatedValue, Double input)
  {
    accumulatedValue.add(input);
    return accumulatedValue;
  }

  @Override
  public MutableDouble merge(MutableDouble accumulatedValue1, MutableDouble accumulatedValue2)
  {
    accumulatedValue1.add(accumulatedValue2);
    return accumulatedValue1;
  }

  @Override
  public Double getOutput(MutableDouble accumulatedValue)
  {
    return accumulatedValue.doubleValue();
  }

  @Override
  public Double getRetraction(Double value)
  {
    return -value;
  }
}
