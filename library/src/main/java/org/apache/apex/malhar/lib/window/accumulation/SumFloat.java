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
import org.apache.commons.lang3.mutable.MutableFloat;

/**
 * Sum Accumulation for floats.
 *
 * @since 3.5.0
 */
public class SumFloat implements Accumulation<Float, MutableFloat, Float>
{
  @Override
  public MutableFloat defaultAccumulatedValue()
  {
    return new MutableFloat(0.);
  }

  @Override
  public MutableFloat accumulate(MutableFloat accumulatedValue, Float input)
  {
    accumulatedValue.add(input);
    return accumulatedValue;
  }

  @Override
  public MutableFloat merge(MutableFloat accumulatedValue1, MutableFloat accumulatedValue2)
  {
    accumulatedValue1.add(accumulatedValue2);
    return accumulatedValue1;
  }

  @Override
  public Float getOutput(MutableFloat accumulatedValue)
  {
    return accumulatedValue.floatValue();
  }

  @Override
  public Float getRetraction(Float value)
  {
    return -value;
  }
}
