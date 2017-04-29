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

import org.apache.apex.malhar.lib.window.MergeAccumulation;

/**
 * Combine Join Accumulation, inner-joins tuples with same type from different streams.
 *
 * @since 3.6.0
 */
public class InnerJoin<T> implements MergeAccumulation<T, T, List<Set<T>>, List<List<T>>>
{

  public InnerJoin()
  {
    //for kryo
  }

  @Override
  public List<Set<T>> accumulate(List<Set<T>> accumulatedValue, T input)
  {
    return accumulateWithIndex(0, accumulatedValue, input);
  }

  @Override
  public List<Set<T>> accumulate2(List<Set<T>> accumulatedValue, T input)
  {
    return accumulateWithIndex(1, accumulatedValue, input);
  }


  public List<Set<T>> accumulateWithIndex(int index, List<Set<T>> accumulatedValue, T input)
  {
    Set<T> accuSet = accumulatedValue.get(index);
    accuSet.add(input);
    accumulatedValue.set(index, accuSet);
    return accumulatedValue;
  }

  @Override
  public List<Set<T>> defaultAccumulatedValue()
  {
    ArrayList<Set<T>> accu = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      accu.add(new HashSet<T>());
    }
    return accu;
  }

  @Override
  public List<Set<T>> merge(List<Set<T>> accumulatedValue1, List<Set<T>> accumulatedValue2)
  {
    for (int i = 0; i < 2; i++) {
      Set<T> accuSet1 = accumulatedValue1.get(i);
      Set<T> accuSet2 = accumulatedValue2.get(i);
      accuSet1.addAll(accuSet2);
      accumulatedValue1.set(i, accuSet1);
    }
    return accumulatedValue1;
  }

  @Override
  public List<List<T>> getOutput(List<Set<T>> accumulatedValue)
  {
    List<List<T>> result = new ArrayList<>();

    // TODO: May need to revisit (use state manager).
    result = getAllCombo(accumulatedValue, result, new ArrayList<T>());

    return result;
  }


  public List<List<T>> getAllCombo(List<Set<T>> accu, List<List<T>> result, List<T> curList)
  {
    if (curList.size() == 2) {
      result.add(curList);
      return result;
    } else {
      for (T item : accu.get(curList.size())) {
        List<T> tempList = new ArrayList<>(curList);
        tempList.add(item);
        result = getAllCombo(accu, result, tempList);
      }
      return result;
    }
  }


  @Override
  public List<List<T>> getRetraction(List<List<T>> value)
  {
    return new ArrayList<>();
  }
}
