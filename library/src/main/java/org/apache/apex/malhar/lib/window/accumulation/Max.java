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

import java.util.Comparator;
import org.apache.apex.malhar.lib.window.Accumulation;

/**
 * Max accumulation.
 *
 * @since 3.5.0
 */
public class Max<T> implements Accumulation<T, T, T>
{

  Comparator<T> comparator;

  public void setComparator(Comparator<T> comparator)
  {
    this.comparator = comparator;
  }

  @Override
  public T defaultAccumulatedValue()
  {
    return null;
  }

  @Override
  public T accumulate(T accumulatedValue, T input)
  {
    if (accumulatedValue == null) {
      return input;
    } else if (comparator != null) {
      return (comparator.compare(input, accumulatedValue) > 0) ? input : accumulatedValue;
    } else if (input instanceof Comparable) {
      return (((Comparable)input).compareTo(accumulatedValue) > 0) ? input : accumulatedValue;
    } else {
      throw new RuntimeException("Tuple cannot be compared");
    }
  }

  @Override
  public T merge(T accumulatedValue1, T accumulatedValue2)
  {
    return accumulate(accumulatedValue1, accumulatedValue2);
  }

  @Override
  public T getOutput(T accumulatedValue)
  {
    return accumulatedValue;
  }

  @Override
  public T getRetraction(T value)
  {
    // TODO: Need to add implementation for retraction.
    return null;
  }
}
