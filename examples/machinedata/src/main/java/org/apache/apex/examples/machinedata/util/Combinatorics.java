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
package org.apache.apex.examples.machinedata.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generate combinations of elements for the given array of elements.
 *
 * Implements nCr = n! / (r! * (n-r)!)
 *
 * @since 0.3.5
 */
public class Combinatorics<T>
{

  private T[] values;
  private int size = -1;
  private List<T> result;
  private Map<Integer, List<T>> resultMap = new HashMap<Integer, List<T>>();
  private int resultMapSize = 0;

  /**
   * Generates all possible combinations with all the sizes.
   *
   * @param values
   */
  public Combinatorics(T[] values)
  {
    this.values = values;
    this.size = -1;
    this.result = new ArrayList<>();
  }

  /**
   * Generates all possible combinations with the given size.
   *
   * @param values
   * @param size
   */
  public Combinatorics(T[] values, int size)
  {
    this.values = values;
    this.size = size;
    this.result = new ArrayList<>();
  }

  public Map<Integer, List<T>> generate()
  {

    if (size == -1) {
      size = values.length;
      for (int i = 1; i <= size; i++) {
        int[] tmp = new int[i];
        Arrays.fill(tmp, -1);
        generateCombinations(0, 0, tmp);
      }
    } else {
      int[] tmp = new int[size];
      Arrays.fill(tmp, -1);
      generateCombinations(0, 0, tmp);
    }
    return resultMap;
  }

  public void generateCombinations(int start, int depth, int[] tmp)
  {
    if (depth == tmp.length) {
      for (int j = 0; j < depth; j++) {
        result.add(values[tmp[j]]);
      }
      resultMap.put(++resultMapSize, result);
      result = new ArrayList<>();
      return;
    }
    for (int i = start; i < values.length; i++) {
      tmp[depth] = i;
      generateCombinations(i + 1, depth + 1, tmp);
    }
  }
}
