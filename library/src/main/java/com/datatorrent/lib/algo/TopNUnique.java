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
import com.datatorrent.api.annotation.OperatorAnnotation;

import com.datatorrent.lib.util.AbstractBaseNUniqueOperatorMap;

/**
 * This operator orders tuples per key and emits the top N unique tuples per key at the end of the window.
 * <p>
 * Orders tuples per key and emits top N unique tuples per key on end of window.
 * </p>
 * <p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
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
 * <b>Specific run time checks are</b>: None<br>
 * <br>
 * </p>
 *
 * @displayName Top N Unique Values Per Key
 * @category Algorithmic
 * @tags filter, rank
 *
 * @since 0.3.2
 */

@OperatorAnnotation(partitionable = false)
public class TopNUnique<K, V> extends AbstractBaseNUniqueOperatorMap<K, V>
{
  /**
   * The output port which emits the top N unique values per key.
   */
  public final transient DefaultOutputPort<HashMap<K, ArrayList<HashMap<V,Integer>>>> top = new DefaultOutputPort<HashMap<K, ArrayList<HashMap<V,Integer>>>>();

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
   * @param tuple
   */
  @Override
  public void emit(HashMap<K, ArrayList<HashMap<V,Integer>>> tuple)
  {
    top.emit(tuple);
  }
  
  /**
   * Top N unique tuples per key
   * @param val
   */
  @Override
  public void setN(int val)
  {
    super.setN(val);
  }
}
