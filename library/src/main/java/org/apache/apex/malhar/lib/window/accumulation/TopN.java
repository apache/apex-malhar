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

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.apex.malhar.lib.window.Accumulation;

/**
 * TopN accumulation
 *
 * @since 3.5.0
 */
public class TopN<T> implements Accumulation<T, List<T>, List<T>>
{
  int n;

  Comparator<T> comparator;

  public void setN(int n)
  {
    this.n = n;
  }

  public void setComparator(Comparator<T> comparator)
  {
    this.comparator = comparator;
  }

  @Override
  public List<T> defaultAccumulatedValue()
  {
    return new LinkedList<>();
  }

  @Override
  public List<T> accumulate(List<T> accumulatedValue, T input)
  {
    int k = 0;
    for (T inMemory : accumulatedValue) {
      if (comparator != null) {
        if (comparator.compare(inMemory, input) < 0) {
          break;
        }
      } else if (input instanceof Comparable) {
        if (((Comparable<T>)input).compareTo(inMemory) > 0) {
          break;
        }
      } else {
        throw new RuntimeException("Tuple cannot be compared");
      }
      k++;
    }
    accumulatedValue.add(k, input);
    if (accumulatedValue.size() > n) {
      accumulatedValue.remove(accumulatedValue.get(accumulatedValue.size() - 1));
    }
    return accumulatedValue;
  }

  @Override
  public List<T> merge(List<T> accumulatedValue1, List<T> accumulatedValue2)
  {
    accumulatedValue1.addAll(accumulatedValue2);
    if (comparator != null) {
      Collections.sort(accumulatedValue1, Collections.reverseOrder(comparator));
    } else {
      Collections.sort(accumulatedValue1, Collections.reverseOrder());
    }
    if (accumulatedValue1.size() > n) {
      return accumulatedValue1.subList(0, n);
    } else {
      return accumulatedValue1;
    }
  }

  @Override
  public List<T> getOutput(List<T> accumulatedValue)
  {
    return accumulatedValue;
  }

  @Override
  public List<T> getRetraction(List<T> accumulatedValue)
  {
    return new LinkedList<>();
  }
}

