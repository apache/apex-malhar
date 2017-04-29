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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.apex.malhar.lib.window.Accumulation;

/**
 * RemoveDuplicates Accumulation.
 * @param <T>
 *
 * @since 3.5.0
 */
public class RemoveDuplicates<T> implements Accumulation<T, Set<T>, List<T>>
{
  @Override
  public Set<T> defaultAccumulatedValue()
  {
    return new HashSet<>();
  }

  @Override
  public Set<T> accumulate(Set<T> accumulatedValue, T input)
  {
    accumulatedValue.add(input);
    return accumulatedValue;
  }

  @Override
  public Set<T> merge(Set<T> accumulatedValue1, Set<T> accumulatedValue2)
  {
    for (T item : accumulatedValue2) {
      accumulatedValue1.add(item);
    }
    return accumulatedValue1;
  }

  @Override
  public List<T> getOutput(Set<T> accumulatedValue)
  {
    if (accumulatedValue == null) {
      return new ArrayList<>();
    } else {
      return new ArrayList<>(accumulatedValue);
    }
  }

  @Override
  public List<T> getRetraction(List<T> value)
  {
    // TODO: Need to add implementation for retraction.
    return new ArrayList<>(value);
  }
}
