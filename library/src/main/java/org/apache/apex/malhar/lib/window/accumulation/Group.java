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

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.malhar.lib.window.Accumulation;

/**
 * Group accumulation.
 *
 * @since 3.5.0
 */
public class Group<T> implements Accumulation<T, List<T>, List<T>>
{
  @Override
  public List<T> defaultAccumulatedValue()
  {
    return new ArrayList<>();
  }

  @Override
  public List<T> accumulate(List<T> accumulatedValue, T input)
  {
    accumulatedValue.add(input);
    return accumulatedValue;
  }

  @Override
  public List<T> merge(List<T> accumulatedValue1, List<T> accumulatedValue2)
  {
    accumulatedValue1.addAll(accumulatedValue2);
    return accumulatedValue1;
  }

  @Override
  public List<T> getOutput(List<T> accumulatedValue)
  {
    return accumulatedValue;
  }

  @Override
  public List<T> getRetraction(List<T> value)
  {
    // TODO: Need to add implementation for retraction.
    return new ArrayList<>();
  }
}
