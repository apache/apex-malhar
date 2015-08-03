/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.multiwindow;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.google.common.base.Function;
import org.apache.commons.lang.ClassUtils;

/**
 *
 * Provides a sliding window class that sorts all incoming tuples within the window and emit them in the right order.
 * <p>
 * Generally, given tuples T, keys K, windows W. All T within Window W are split into |K| buckets and <br>
 * sort the bucket in the order that Tp_Ki < Tq_Ki if (comparator.compare(Tp_Ki, Tq_Ki) < 0 || ((Tp_Ki instance of Comparable) && Tp_Ki.compareTo(Tq_Ki) <0))</p>
 *
 * <b>Properties</b>:<br>
 * <b>T</b> is the tuple object the operator can process <br>
 * <b>K</b> is the key object used to categorize the tuples within the sliding window<br>
 * <b>function</b>: is used transform the tuple T to group key K. It's used to split all tuples into |K| group and sorted them in the group<br>
 * by default: function is SingleKeyMappingFunction which map all t to null (all tuples are grouped into one group)
 * <br><b>comparator</b>: is used to determine the order of the tuple<br>
 * by default: comparator is null which means the tuple must be comparable
 * <p></p>
 *
 * @displayName Sorted Moving Window
 * @category Stats and Aggregations
 * @tags sort, list, function, sliding window
 * @since 0.9.2
 */
public class SortedMovingWindow<T, K> extends AbstractSlidingWindow<T, List<T>>
{
  /**
   * Output port to emit sorted output.
   */
  public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * Output port to emit error output.
   */
  @OutputPortFieldAnnotation(error = true)
  public transient DefaultOutputPort<T> errorOutput = new DefaultOutputPort<T>();

  private Map<K, PriorityQueue<T>> sortedListInSlidingWin = new HashMap<K, PriorityQueue<T>>();

  private List<T> tuplesInCurrentStreamWindow = new LinkedList<T>();

  @NotNull
  private Function<T, K> function = new SingleKeyMappingFunction<T, K>();

  private Comparator<T> comparator = null;

  @Override
  protected void processDataTuple(T tuple)
  {
    tuplesInCurrentStreamWindow.add(tuple);
    K key = function.apply(tuple);
    PriorityQueue<T> sortedList = sortedListInSlidingWin.get(key);
    if (sortedList == null) {
      sortedList = new PriorityQueue<T>(10, comparator);
      sortedListInSlidingWin.put(key, sortedList);
    }
    sortedList.add(tuple);
  }


  @Override
  public List<T> createWindowState()
  {
    return tuplesInCurrentStreamWindow;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void endWindow()
  {
    super.endWindow();
    tuplesInCurrentStreamWindow = new LinkedList<T>();
    if(lastExpiredWindowState == null){
      // not ready to emit value or empty in a certain window
      return;
    }
    // Assumption: the expiring tuple and any tuple before are already sorted. So it's safe to emit tuples from sortedListInSlidingWin till the expiring tuple
    for (T expiredTuple : lastExpiredWindowState) {
      // Find sorted list for the given key
      PriorityQueue<T> sortedListForE = sortedListInSlidingWin.get(function.apply(expiredTuple));
      for (Iterator<T> iterator = sortedListForE.iterator(); iterator.hasNext();) {
        T minElemInSortedList = iterator.next();
        int k = 0;
        if (comparator == null) {
          if (expiredTuple instanceof Comparable) {
            k = ((Comparable<T>) expiredTuple).compareTo(minElemInSortedList);
          } else {
            errorOutput.emit(expiredTuple);
            throw new IllegalArgumentException("Operator \"" + ClassUtils.getShortClassName(this.getClass()) + "\" encounters an invalid tuple " + expiredTuple + "\nNeither the tuple is comparable Nor Comparator is specified!");
          }
        } else {
          k = comparator.compare(expiredTuple, minElemInSortedList);
        }
        if (k < 0) {
          // If the expiring tuple is less than the first element of the sorted list. No more tuples to emit
          break;
        } else {
          // Emit the element in sorted list if it's less than the expiring tuple
          outputPort.emit(minElemInSortedList);
          // remove the element from the sorted list
          iterator.remove();
        }
      }
    }
  }

  /**
   * Default grouping function that map all tuples into single group
   * @param <T>
   * @param <K>
   */
  private static class SingleKeyMappingFunction<T, K> implements Function<T, K>
  {
    @Override
    public K apply(T input)
    {
      return null;
    }
  }

  public void setComparator(Comparator<T> comparator)
  {
    this.comparator = comparator;
  }

  public void setFunction(Function<T, K> function)
  {
    this.function = function;
  }

}
