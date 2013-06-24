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

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */


import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.AbstractBaseSortOperator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

/**
 * <b>performance can be further improved</b>
 * Incoming sorted list is merged into already existing sorted list in a descending order. The input list is expected to be sorted. At the end of the window the resultant sorted
 * list is emitted on the output ports<b>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects ArrayList&lt;K&gt;<br>
 * <b>sort</b>: emits ArrayList&lt;K&gt;<br>
 * <b>sorthash</b>: emits HashMap&lt;K,Integer&gt;<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>:<br>
 * <b>Specific run time checks</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for MergeSortDesc&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 2.5 Million tuples/s on average</b></td><td>All tuples inserted one at a time</td>
 * <td>In-bound throughput (i.e. total number of tuples in the window) is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for MergeSortDesc&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>datalsit</i>(ArrayList&lt;K&gt;)</th><th><i>sort</i>(ArrayList&lt;K&gt;)</th><th><i>sorthash</i>(HashMap&lt;K,Integer&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td></td><td>[-4,2,20]</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>[-10,-5,-4]</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>[1,2,3,3,10,15,100]</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>[1,1,2]</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>[15,20]</td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>[100,20,20,15,15,3,3,2,2,2,1,1,1,-4,-4,-5,-10,-10]</td>
 * <td>{100=1,20=2,15=2,3=2,2=3,1=3,-4=2,-5=1,-10=2}</td></tr>
 * </table>
 * <br>
 * <br>
 */
public class MergeSortDesc<K> extends AbstractBaseSortOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<ArrayList<K>> data = new DefaultInputPort<ArrayList<K>>()
  {
    @Override
    public void process(ArrayList<K> tuple)
    {
      processTuple(tuple);
    }
  };
  @OutputPortFieldAnnotation(name = "sort")
  public final transient DefaultOutputPort<ArrayList<K>> sort = new DefaultOutputPort<ArrayList<K>>();
  @OutputPortFieldAnnotation(name = "sorthash", optional = true)
  public final transient DefaultOutputPort<HashMap<K, Integer>> sorthash = new DefaultOutputPort<HashMap<K, Integer>>();

  /*
   * <b>Currently implemented with individual keys inserted. Needs to be reimplemented as a merge sort</b>
   *
   */
  @Override
  public void processTuple(ArrayList<K> tuple)
  {
    super.processTuple(tuple);
  }

  @Override
  public void initializeQueue()
  {
    pqueue = new PriorityQueue<K>(getSize(), new ReversibleComparator<K>(false));
  }


  @Override
  public boolean doEmitList()
  {
    return sort.isConnected();
  }

  @Override
  public boolean doEmitHash()
  {
    return sorthash.isConnected();
  }

  @Override
  public void emitToList(ArrayList<K> list)
  {
    sort.emit(list);
  }

  @Override
  public void emitToHash(HashMap<K,Integer> map)
  {
    sorthash.emit(map);
  }
}
