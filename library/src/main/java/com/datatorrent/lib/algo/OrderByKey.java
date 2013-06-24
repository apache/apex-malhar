/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseKeyValueOperator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang.mutable.MutableInt;

/**
 * Order by ascending is done on the incoming stream based on val, and result is emitted on end of window<p>
 * This is an end of window module. At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>ordered_count</b>: emits HashMap&lt;V,Integer&gt;<br>
 * <b>ordered_list</b>: emits HashMap&lt;K,V&gt;<br>
 * <br>
 * <b>Properties</b>:
 * <b>orderby</b>: The key to order by<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * orderBy cannot be empty<br>
 * <br>
 * <b>Specific run time checks are</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for OrderByKey&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 5 Million K,V pairs/s</b></td><td>All tuples are emitted on port ordered_list, and one per value is emitted on port ordered_count</td>
 * <td>In-bound throughput and value diversity is the main determinant of performance.
 * The benchmark was done with immutable objects. If K or V are mutable the benchmark may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); orderBy=a</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for OrderByKey&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>ordered_count</i>(HashMap&lt;V,Integer&gt;)</th><th><i>ordered_list</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><</tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5,c=6}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=50,c=16}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5,c=6}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1,b=2,c=3}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1,b=7,c=4}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,b=23,c=33}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5,c=6}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{e=2,b=5,c=6}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=-1,b=-5,c=6}</td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{-1=1}<br>{1=2}<br>{2=2}<br>{3=1}</td>
 * <td>{b=-5, c=6, a=-1}<br>{b=2, c=3, a=1}<br>{b=7, c=4, a=1}<br>{b=5, c=6, a=2}<br>{b=5, c=6, a=2}<br>{b=23, c=33, a=3}<br></td></tr>
 * </table>
 * <br>
 * <br>
 */
public class OrderByKey<K,V> extends BaseKeyValueOperator<K,V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>()
  {
    /**
     * process the tuple is orderby val exists.
     */
    @Override
    public void process(HashMap<K,V> tuple)
    {
      V val = tuple.get(orderby);
      if (val == null) {
        // emit error tuple?
        return;
      }
      boolean first = false;
      if (ordered_count.isConnected()) {
        MutableInt count = countmap.get(val);
        if (count == null) {
          count = new MutableInt(0);
          countmap.put(cloneValue(val), count);
          first = true;
        }
        count.increment();
      }
      if (ordered_list.isConnected()) {
        ArrayList<HashMap<K,V>> list = smap.get(val);
        if (list == null) {
          list = new ArrayList<HashMap<K,V>>();
          smap.put(cloneValue(val), list);
          first = true;
        }
        list.add(cloneTuple(tuple));
      }
      if (first) {
        pqueue.add(cloneValue(val));
      }
    }
  };

  @OutputPortFieldAnnotation(name = "ordered_list")
  public final transient DefaultOutputPort<HashMap<K,V>> ordered_list = new DefaultOutputPort<HashMap<K,V>>();
  @OutputPortFieldAnnotation(name = "ordered_count")
  public final transient DefaultOutputPort<HashMap<V,Integer>> ordered_count = new DefaultOutputPort<HashMap<V,Integer>>();

  @NotNull()
  K orderby = null;
  protected PriorityQueue<V> pqueue = null;
  protected HashMap<V,MutableInt> countmap = new HashMap<V,MutableInt>();
  protected HashMap<V,ArrayList<HashMap<K,V>>> smap = new HashMap<V,ArrayList<HashMap<K,V>>>();

  /**
   * getter function for orderby
   * @return orderby
   */
  @NotNull()
  public K getOrderby()
  {
    return orderby;
  }

  /**
   * setter function for orderby val
   * @param str
   */
  public void setOrderby(K str)
  {
    orderby = str;
  }

  /**
   * First cut of the priority queue in ascending order
   */
  public void initializePriorityQueue() {
    pqueue = new PriorityQueue<V>(5);
  }

  /**
   * Sets up the priority queue
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    if (getOrderby() == null) {
      throw new IllegalArgumentException("Orderby key not set");
    }
    initializePriorityQueue();
  }

  /**
   * Initializes queue once in the life of the operator
   */
  @Override
  public void beginWindow(long windowId)
  {
    if (pqueue == null) {
      initializePriorityQueue();
    }
  }

  /**
   * Emits tuples
   * Clears internal data
   */
  @Override
  public void endWindow()
  {
    V val;
    while ((val = pqueue.poll()) != null) {
      if (ordered_count.isConnected()) {
        HashMap<V, Integer> tuple = new HashMap<V, Integer>(1);
        tuple.put(val, countmap.get(val).toInteger());
        ordered_count.emit(tuple);
      }
      if (ordered_list.isConnected()) {
        ArrayList<HashMap<K, V>> list = smap.get(val);
        for (HashMap<K, V> o: list) {
          ordered_list.emit(o);
        }
      }
    }
    pqueue.clear();
    countmap.clear();
    smap.clear();
  }
}
