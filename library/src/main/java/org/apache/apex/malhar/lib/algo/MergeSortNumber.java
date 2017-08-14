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
package org.apache.apex.malhar.lib.algo;

import java.util.ArrayList;

import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This unifier takes sorted lists of tuples each window and merges them into one large sorted list at the end of
 * each window.
 * <p>
 * Incoming sorted list is merged into already existing sorted list. The input list is expected to be sorted. <b>
 * At the end of the window, merged sorted list is emitted on sort output port. <br>
 * <br>
 * <b>Notes : </b> <br>
 * Get unifier instance must return instance of sub class itself, since merge operator
 * id unifier on output port. <br>
 * <br>
 *  <b>StateFull : Yes</b>, Sorted listed are merged over application window can be > 1. <br>
 *  <b>Partitions : Yes</b>, Operator itself is used as unfier on output port.
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects ArrayList&lt;K&gt;<br>
 * <b>sort</b>: emits ArrayList&lt;K&gt;<br>
 * <br>
 * <b>Abstract Methods: </b><br>
 * 1. compare : K type value compare criteria for sort.
 * 2. getUnifierInstance : Get unifier operator instance for output port, (must return self instance).
 * </p>
 * @displayName Merge Sorted Lists (Number)
 * @category Stream Manipulators
 * @tags rank, numeric
 *
 * @since 0.3.3
 * @deprecated
 */
@Deprecated
@OperatorAnnotation(partitionable = true)
public class MergeSortNumber<V extends Number> extends MergeSort<V>
{
  /**
   * Ascending/Desending flag;
   */
  private boolean ascending = true;

  /**
   * sort function.
   */
  @SuppressWarnings("unchecked")
  public V[] compare(V val1, V val2)
  {
    V[] result = (V[])new Number[2];
    if (ascending) {
      if (val1.doubleValue() < val2.doubleValue()) {
        result[0] = val1;
        result[1] = val2;
      } else {
        result[0] = val2;
        result[1] = val1;
      }
    } else {
      if (val1.doubleValue() < val2.doubleValue()) {
        result[0] = val2;
        result[1] = val1;
      } else {
        result[0] = val1;
        result[1] = val2;
      }
    }
    return result;
  }

  /**
   *  Merge class itself is unifier.
   */
  public Unifier<ArrayList<V>> getUnifierInstance()
  {
    return new MergeSortNumber<V>();
  }

  public boolean isAscending()
  {
    return ascending;
  }

  public void setAscending(boolean ascending)
  {
    this.ascending = ascending;
  }
}
