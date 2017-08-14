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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This is a base class implementation of a unifier which merges sorted lists of tuples.&nbsp;
 * The operator takes sorted lists as input tuples each window.&nbsp;
 * At the end of each application window input tuples are merged into one large sorted list and emitted.&nbsp;
 * Subclasses must implement the comparison method used for sorting.
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
 *
 * @displayName Merge Sorted Lists (Generic)
 * @category Algorithmic
 * @tags rank
 * </p>
 *
 * @since 0.3.3
 * @deprecated
 */
@Deprecated
@OperatorAnnotation(partitionable = true)
public abstract class MergeSort<K>  implements Operator, Unifier<ArrayList<K>>
{
  /**
   * Sorted merged list.
   */
  private ArrayList<K> mergedList = null;

  /**
   * The input port which receives lists to be merged and sorted.
   */
  public final transient DefaultInputPort<ArrayList<K>> data = new DefaultInputPort<ArrayList<K>>()
  {
    /**
     * Merge incoming tuple.
     */
    @Override
    public void process(ArrayList<K> tuple)
    {
      mergedList = processMergeList(mergedList, tuple);
    }
  };

  /**
   * The output port which emits merged and sorted lists.
   */
  public final transient DefaultOutputPort<ArrayList<K>> sort = new DefaultOutputPort<ArrayList<K>>()
  {
    @Override
    public Unifier<ArrayList<K>> getUnifier()
    {
      return getUnifierInstance();
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub
  }

  @Override
  public void beginWindow(long windowId)
  {
    mergedList = null;
  }

  @Override
  public void endWindow()
  {
    sort.emit(mergedList);
    mergedList = null;
  }

  /**
   *  Sorted parameter list are merged into sorted merged output list.
   *
   * @param list1 sorted list aggregated by operator
   * @param list2 Input port sorted list to be merged.
   * @return sorted merged output list.
   */
  protected ArrayList<K> processMergeList(ArrayList<K> list1,
      ArrayList<K> list2)
  {
    // null lists
    if (list1 == null) {
      return list2;
    }
    if (list2 == null) {
      return list1;
    }

    // Create output list
    ArrayList<K> result = new ArrayList<K>();
    int index1 = 0;
    int index2 = 0;
    while (true) {

      // list1 is exhausted
      if (index1 == list1.size()) {
        while (index2 < list2.size()) {
          result.add(list2.get(index2++));
        }
        break;
      }

      // list2 is exhausted
      if (index2 == list2.size()) {
        while (index1 < list1.size()) {
          result.add(list1.get(index1++));
        }
        break;
      }

      // compare values
      K val1 = list1.get(index1++);
      K val2 = list2.get(index2++);
      K[] vals = compare(val1, val2);
      result.add(vals[0]);
      if (vals[1] != null) {
        result.add(vals[1]);
      }
    }

    // done
    return result;
  }

  /**
   * Unifier process function implementation.
   */
  @Override
  public void process(ArrayList<K> tuple)
  {
    mergedList = processMergeList(mergedList, tuple);
  }

  /**
   * abstract sort function to be implemented by sub class.
   */
  public abstract K[] compare(K val1, K val2);

  /**
   *  Get output port unifier instance, sub class should return new instance of itself.
   */
  public abstract Unifier<ArrayList<K>> getUnifierInstance();
}
