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
package org.apache.apex.malhar.stream.sample.complete;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.window.Accumulation;

/**
 * Specialized TopNByKey accumulation for AutoComplete Example.
 *
 * @since 3.5.0
 */
public class TopNByKey implements
    Accumulation<CompletionCandidate, Map<String, Long>, List<CompletionCandidate>>
{
  int n = 10;

  Comparator comparator;

  public void setN(int n)
  {
    this.n = n;
  }

  public void setComparator(Comparator comparator)
  {
    this.comparator = comparator;
  }

  @Override
  public Map<String, Long> defaultAccumulatedValue()
  {
    return new HashMap<>();
  }

  /**
   * Accumulate the input. Update the entry in the Accumulation Map if the key of the input is existed, create a
   * new entry otherwise.
   * @param accumulatedValue
   * @param input
   * @return
   */
  @Override
  public Map<String, Long> accumulate(Map<String, Long> accumulatedValue, CompletionCandidate input)
  {
    accumulatedValue.put(input.getValue(), input.getCount());
    return accumulatedValue;
  }

  /**
   * Merge two Maps together. For every key, keep the larger value in the resulted Map.
   * @param accumulatedValue1
   * @param accumulatedValue2
   * @return
   */
  @Override
  public Map<String, Long> merge(Map<String, Long> accumulatedValue1, Map<String, Long> accumulatedValue2)
  {
    for (Map.Entry<String, Long> entry : accumulatedValue2.entrySet()) {
      if (accumulatedValue1.containsKey(entry.getKey()) && accumulatedValue1.get(entry.getKey()) > entry.getValue()) {
        continue;
      }
      accumulatedValue1.put(entry.getKey(), entry.getValue());
    }
    return accumulatedValue1;
  }

  /**
   * Loop through the Accumulation Map to get the top n entries based on their values, return a list containing
   * those entries.
   * @param accumulatedValue
   * @return
   */
  @Override
  public List<CompletionCandidate> getOutput(Map<String, Long> accumulatedValue)
  {
    LinkedList<CompletionCandidate> result = new LinkedList<>();
    for (Map.Entry<String, Long> entry : accumulatedValue.entrySet()) {
      int k = 0;
      for (CompletionCandidate inMemory : result) {
        if (entry.getValue() > inMemory.getCount()) {
          break;
        }
        k++;
      }
      result.add(k, new CompletionCandidate(entry.getKey(), entry.getValue()));
      if (result.size() > n) {
        result.remove(result.get(result.size() - 1));
      }
    }
    return result;
  }

  @Override
  public List<CompletionCandidate> getRetraction(List<CompletionCandidate> value)
  {
    return new LinkedList<>();
  }
}
