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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.Accumulation;

/**
 * Generalized TopNByKey accumulation
 *
 * @since 3.5.0
 */
public class TopNByKey<K, V> implements
    Accumulation<KeyValPair<K, V>, Map<K, V>, List<KeyValPair<K, V>>>
{
  int n = 10;

  Comparator<V> comparator;

  public void setN(int n)
  {
    this.n = n;
  }

  public void setComparator(Comparator<V> comparator)
  {
    this.comparator = comparator;
  }

  @Override
  public Map<K, V> defaultAccumulatedValue()
  {
    return new HashMap<>();
  }

  @Override
  public Map<K, V> accumulate(Map<K, V> accumulatedValue, KeyValPair<K, V> input)
  {
    accumulatedValue.put(input.getKey(), input.getValue());
    return accumulatedValue;
  }

  @Override
  public Map<K, V> merge(Map<K, V> accumulatedValue1, Map<K, V> accumulatedValue2)
  {
    for (Map.Entry<K, V> entry : accumulatedValue2.entrySet()) {
      if (!accumulatedValue1.containsKey(entry.getKey())) {
        accumulatedValue1.put(entry.getKey(), entry.getValue());
      } else if (comparator != null) {
        if (comparator.compare(entry.getValue(), accumulatedValue1.get(entry.getKey())) > 0) {
          accumulatedValue1.put(entry.getKey(), entry.getValue());
        }
      } else if (entry.getValue() instanceof Comparable) {
        if (((Comparable<V>)entry.getValue()).compareTo(accumulatedValue1.get(entry.getKey())) > 0) {
          accumulatedValue1.put(entry.getKey(), entry.getValue());
        }
      }
    }
    return accumulatedValue1;
  }

  @Override
  public List<KeyValPair<K, V>> getOutput(Map<K, V> accumulatedValue)
  {
    LinkedList<KeyValPair<K, V>> result = new LinkedList<>();
    for (Map.Entry<K, V> entry : accumulatedValue.entrySet()) {
      int k = 0;
      for (KeyValPair<K, V> inMemory : result) {
        if (comparator != null) {
          if (comparator.compare(entry.getValue(), inMemory.getValue()) > 0) {
            break;
          }
        } else if (entry.getValue() instanceof Comparable) {
          if (((Comparable<V>)entry.getValue()).compareTo(inMemory.getValue()) > 0) {
            break;
          }
        }
        k++;
      }
      result.add(k, new KeyValPair<K, V>(entry.getKey(), entry.getValue()));
      if (result.size() > n) {
        result.remove(result.get(result.size() - 1));
      }
    }
    return result;
  }

  @Override
  public List<KeyValPair<K, V>> getRetraction(List<KeyValPair<K, V>> value)
  {
    // TODO: Need to add implementation for retraction.
    return new LinkedList<>();
  }
}
