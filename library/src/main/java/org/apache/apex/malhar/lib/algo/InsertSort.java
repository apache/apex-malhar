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
import java.util.HashMap;

import org.apache.apex.malhar.lib.util.AbstractBaseSortOperator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

/**
 * This operator takes the values it receives each window and outputs them in ascending order at the end of each window.
 * <p>
 * Takes a stream of key value pairs via input port "data". The incoming tuple
 * is merged into already existing sorted list. At the end of the window the
 * entire sorted list is emitted on output port "sort"
 * </p>
 * <p>
 * <br>
 * <b>StateFull : Yes, </b> tuple are compare across application window(s). <br>
 * <b>Partitions : Yes, </b> the operator itself serves as the unifier.
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects K<br>
 * <b>datalist</b>: expects ArrayList&lt;K&gt;<br>
 * <b>sortlist</b>: emits ArrayList&lt;K&gt;, must be connected<br>
 * <br>
 * </p>
 *
 * @displayName Sort Ascending
 * @category Stream Manipulators
 * @tags rank, sort
 *
 * @since 0.3.3
 */
//
// TODO: Override PriorityQueue and rewrite addAll to insert with location
//
public class InsertSort<K> extends AbstractBaseSortOperator<K> implements Unifier<ArrayList<K>>
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
   * The output port on which a sorted ascending list of tuples is emitted.
   */
  public final transient DefaultOutputPort<ArrayList<K>> sort = new DefaultOutputPort<ArrayList<K>>()
  {
    @Override
    public Unifier<ArrayList<K>> getUnifier()
    {
      InsertSort<K> ret = new InsertSort<K>();
      return ret;
    }
  };

  @Override
  public void emitToList(ArrayList<K> list)
  {
    sort.emit(list);
  }

  @Override
  public void emitToHash(HashMap<K, Integer> map)
  {
  }

  @Override
  public boolean doEmitList()
  {
    return true;
  }

  @Override
  public boolean doEmitHash()
  {
    return false;
  }

  @Override
  public void process(ArrayList<K> tuple)
  {
    processTuple(tuple);
  }
}
