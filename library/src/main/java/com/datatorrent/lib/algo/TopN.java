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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.OperatorAnnotation;

import com.datatorrent.lib.util.AbstractBaseNNonUniqueOperatorMap;

/**
 * This operator orders tuples per key and emits the top N tuples per key at the end of the window.
 * <p>
 * Orders tuples per key and emits top N tuples per key on end of window.
 * </p>
 * <p>
 * This is an end of window module.<br>
 * <br>
 * <b>StateFull : Yes, </b> Tuple are aggregated across application window(s). <br>
 * <b>Partitions : Yes, </b> Top values are unified on output port. <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input data port expects HashMap&lt;K,V&gt;<br>
 * <b>top</b>: Output data port, emits HashMap&lt;K, ArrayList&lt;V&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * N: Has to be >= 1<br>
 * <br>
 * </p>
 *
 * @displayName Top N Values Per Key
 * @category Algorithmic
 * @tags filter, rank
 *
 * @since 0.3.3
 */

@OperatorAnnotation(partitionable = true)
public class TopN<K, V> extends AbstractBaseNNonUniqueOperatorMap<K,V> implements Unifier<HashMap<K, ArrayList<V>>>
{
  /**
   * The output port which emits the top N values per key.
   */
  public final transient DefaultOutputPort<HashMap<K, ArrayList<V>>> top = new DefaultOutputPort<HashMap<K, ArrayList<V>>>()
  {
    @Override
    public Unifier<HashMap<K, ArrayList<V>>> getUnifier()
    {
      TopN<K, V> unifier = new TopN<K, V>();
      unifier.setN(getN());
      return unifier;
    }
  };

  /**
   * returns true
   * @return true
   */
  @Override
  public boolean isAscending()
  {
    return true;
  }

  /**
   * Emits tuple on port "top"
   */
  @Override
  public void emit(HashMap<K, ArrayList<V>> tuple)
  {
    top.emit(tuple);
  }

  @Override
  public void process(HashMap<K, ArrayList<V>> tuple)
  {
    for (Map.Entry<K, ArrayList<V>> entry : tuple.entrySet()) {
      for (V value : entry.getValue()) {
        HashMap<K, V> item = new HashMap<K, V>();
        item.put(entry.getKey(), value);
        this.processTuple(item);
      }
    }
  }
  
  /**
   * Top N tuples per key
   * @param val
   */
  @Override
  public void setN(int val)
  {
    super.setN(val);
  }
}
