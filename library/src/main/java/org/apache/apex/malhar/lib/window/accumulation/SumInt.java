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
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Sum accumulation for integers.
 *
 * @since 3.5.0
 */
public class SumInt implements Accumulation<Integer, MutableInt, Integer>
{
  @Override
  public MutableInt defaultAccumulatedValue()
  {
    return new MutableInt(0);
  }

  @Override
  public MutableInt accumulate(MutableInt accumulatedValue, Integer input)
  {
    accumulatedValue.add(input);
    return accumulatedValue;
  }

  @Override
  public MutableInt merge(MutableInt accumulatedValue1, MutableInt accumulatedValue2)
  {
    accumulatedValue1.add(accumulatedValue2);
    return accumulatedValue1;
  }

  @Override
  public Integer getOutput(MutableInt accumulatedValue)
  {
    return accumulatedValue.intValue();
  }

  @Override
  public Integer getRetraction(Integer value)
  {
    return -value;
  }
}
