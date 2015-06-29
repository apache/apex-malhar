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

import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.lib.util.AbstractBaseNNonUniqueOperatorMap;

/**
 * This operator takes an input stream of key value pairs is ordered by key,
 * and the bottom N of the ordered tuples per key are emitted on port "bottom" at the end of window.
 * <p>
 * Input stream of key value pairs is ordered by key, and bottom N of the
 * ordered tuples per key are emitted on port "bottom" at the end of window
 * </p>
 * <p>
 * This is an end of window operator. At the end of window all data is flushed.
 * Thus the data set is windowed and no history is kept of previous windows<br>
 * The operator assumes that the key, val pairs in the incoming tuple is
 * immutable. If the tuple is mutable users should override cloneKey(), and
 * cloneValue()<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V&gt;<br>
 * <b>bottom</b>: emits HashMap&lt;K, ArrayList&lt;V&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * N: Has to be >= 1<br>
 * <br>
 * </p>
 *
 * @displayName Bottom N
 * @category Algorithmic
 * @tags filter, rank, key value
 *
 * @since 0.3.3
 */
public class BottomNMap<K, V> extends AbstractBaseNNonUniqueOperatorMap<K, V>
{
  /**
   * The output port on which the bottom N tuples for each key are emitted.
   */
  public final transient DefaultOutputPort<HashMap<K, ArrayList<V>>> bottom = new DefaultOutputPort<HashMap<K, ArrayList<V>>>()
  {
    @Override
    public Unifier<HashMap<K, ArrayList<V>>> getUnifier()
    {
      BottomNUnifier<K, V> unifier = new BottomNUnifier<K, V>();
      unifier.setN(getN());
      return unifier;
    }
  };

  /**
   * Ascending is set to false as we are looking for Bottom N
   *
   * @return false
   */
  @Override
  public boolean isAscending()
  {
    return false;
  }

  /**
   * Emits tuple to port "bottom"
   *
   * @param tuple
   */
  @Override
  public void emit(HashMap<K, ArrayList<V>> tuple)
  {
    bottom.emit(tuple);
  }
  
  /**
   * @param val Bottom N values to be returned
   */
  @Override
  public void setN(int val)
  {
    super.setN(val);
  }
}
