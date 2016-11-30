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
import org.apache.commons.lang3.mutable.MutableLong;

/**
 * Sum accumulation for longs.
 *
 * @since 3.5.0
 */
public class SumLong implements Accumulation<Long, MutableLong, Long>
{
  @Override
  public MutableLong defaultAccumulatedValue()
  {
    return new MutableLong(0L);
  }

  @Override
  public MutableLong accumulate(MutableLong accumulatedValue, Long input)
  {
    accumulatedValue.add(input);
    return accumulatedValue;
  }

  @Override
  public MutableLong merge(MutableLong accumulatedValue1, MutableLong accumulatedValue2)
  {
    accumulatedValue1.add(accumulatedValue2);
    return accumulatedValue1;
  }

  @Override
  public Long getOutput(MutableLong accumulatedValue)
  {
    return accumulatedValue.longValue();
  }

  @Override
  public Long getRetraction(Long value)
  {
    return -value;
  }
}
