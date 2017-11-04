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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Sort accumulation
 *
 * @since 3.8.0
 */
@InterfaceStability.Evolving
public class Sort<T> implements Accumulation<T, List<T>, List<T>>
{
  boolean reverseSort;
  Comparator<T> comparator;
  Comparator<T> reverseComparator;

  public Sort()
  {
    //for kryo
  }

  /**
   * @param reverseSort
   *          sort in order opposite to how the comparator would sort
   * @param comparator
   *          comparator to sort the tuples
   */
  public Sort(final boolean reverseSort, final Comparator<T> comparator)
  {
    this.reverseSort = reverseSort;
    this.comparator = comparator;
    this.reverseComparator = Collections.reverseOrder(comparator);
  }

  @Override
  public List<T> defaultAccumulatedValue()
  {
    return new ArrayList<T>();
  }

  @Override
  public List<T> accumulate(List<T> accumulatedValue, T input)
  {
    if (comparator == null) {
      throw new RuntimeException("Comparator not provided, Tuple cannot be compared");
    }
    Comparator<T> accComparator = reverseSort ? reverseComparator : comparator;
    insertElement(accumulatedValue, input, accComparator);
    return accumulatedValue;
  }

  @Override
  public List<T> merge(List<T> accumulatedValue1, List<T> accumulatedValue2)
  {
    if (comparator == null) {
      throw new RuntimeException("Comparator not provided, Tuple cannot be compared");
    }
    Comparator<T> accComparator = reverseSort ? reverseComparator : comparator;
    for (T t : accumulatedValue2) {
      insertElement(accumulatedValue1, t, accComparator);
    }
    return accumulatedValue1;
  }

  private void insertElement(List<T> accumulatedValue, T element, Comparator<T> comparator)
  {
    //binarySearch returns location if input exists else returns (-(insertion point) - 1)
    int index = Collections.binarySearch(accumulatedValue, element, comparator);
    index = index >= 0 ? index : (-index - 1);
    accumulatedValue.add(index, element);
  }

  @Override
  public List<T> getOutput(List<T> accumulatedValue)
  {
    return accumulatedValue;
  }

  @Override
  public List<T> getRetraction(List<T> accumulatedValue)
  {
    return new ArrayList<T>();
  }
}
