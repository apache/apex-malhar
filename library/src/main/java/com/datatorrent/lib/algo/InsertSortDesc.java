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
package com.datatorrent.lib.algo;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.lib.util.AbstractBaseSortOperator;
import com.datatorrent.lib.util.ReversibleComparator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

/**
 * This operator takes the values it receives each window and outputs them in ascending order at the end of each window.
 * <p>
 * Incoming tuple is inserted into already existing sorted list in a descending order. At the end of the window the resultant sorted list is emitted on the output ports.
 * </p>
 * <p>
 * <br>
 * <b>StateFull : Yes, </b> tuple are compare across application window(s). <br>
 * <b>Partitions : No, </b> will yield wrong results. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects K<br>
 * <b>datalist</b>: expects ArrayList&lt;K&gt;<br>
 * <b>sortlist</b>: emits ArrayList&lt;K&gt;<br>
 * <b>sorthash</b>: emits HashMap&lt;K,Integer&gt;<br>
 * <br>
 * <br>
 * </p>
 * @displayName Sort Descending
 * @category Algorithmic
 * @tags rank
 *
 * @since 0.3.2
 */
//
// TODO: Override PriorityQueue and rewrite addAll to insert with location
//
@OperatorAnnotation(partitionable = false)
public class InsertSortDesc<K> extends AbstractBaseSortOperator<K>
{
  /**
   * The input port on which individual tuples are received for sorting.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    /**
     * Adds tuple to sorted queue
     */
    @Override
    public void process(K tuple)
    {
      processTuple(tuple);
    }
  };
  /**
   * The input port on which lists of tuples are received for sorting.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<ArrayList<K>> datalist = new DefaultInputPort<ArrayList<K>>()
  {
    /**
     * Adds tuples to sorted queue
     */
    @Override
    public void process(ArrayList<K> tuple)
    {
      processTuple(tuple);
    }
  };

  /**
   * The output port on which a sorted descending list of tuples is emitted.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<ArrayList<K>> sort = new DefaultOutputPort<ArrayList<K>>();
  @OutputPortFieldAnnotation(optional = true)
  /**
   * This output port emits a map from tuples to a count of the number of times each tuple occurred in the application window.
   */
  public final transient DefaultOutputPort<HashMap<K, Integer>> sorthash = new DefaultOutputPort<HashMap<K, Integer>>();

  @Override
  public void initializeQueue()
  {
    pqueue = new PriorityQueue<K>(getSize(), new ReversibleComparator<K>(false));
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
}
