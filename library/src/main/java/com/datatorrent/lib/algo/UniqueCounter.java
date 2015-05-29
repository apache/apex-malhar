/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
 * limitations under the License.
 */
package com.datatorrent.lib.algo;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

import com.datatorrent.lib.util.BaseUniqueKeyCounter;
import com.datatorrent.lib.util.UnifierHashMapSumKeys;

/**
 * This operator counts the number of times a key exists in a window.&nbsp;A map from keys to counts is emitted at the end of each window.
 * <p>
 * Counts the number of times a key exists in a window; Count is emitted at end of window in a single HashMap.
 * </p>
 * <p>
 * This is an end of window operator<br>
 * <br>
 * <b>StateFull : yes, </b> Tuples are aggregated over application window(s). <br>
 * <b>Partitions : Yes, </b> Unique count is unified at output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects K<br>
 * <b>count</b>: emits HashMap&lt;K,Integer&gt;<br>
 * <b>Properties</b>: None<br>
 * <br>
 * </p>
 *
 * @displayName Count Unique Keys
 * @category Algorithmic
 * @tags count
 *
 * @since 0.3.2
 */

@OperatorAnnotation(partitionable = true)
public class UniqueCounter<K> extends BaseUniqueKeyCounter<K>
{
  /**
   * The input port which receives incoming tuples.
   */
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    /**
     * Reference counts tuples
     */
    @Override
    public void process(K tuple)
    {
      processTuple(tuple);
    }

  };

  /**
   * The output port which emits a map from keys to the number of times they occurred within an application window.
   */
  public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>()
  {
    @Override
    public Unifier<HashMap<K, Integer>> getUnifier()
    {
      UnifierHashMapSumKeys unifierHashMapSumKeys =  new UnifierHashMapSumKeys<K, Integer>();
      unifierHashMapSumKeys.setType(Integer.class);
      return unifierHashMapSumKeys;
    }
  };

  /**
   * Emits one HashMap as tuple
   */
  @Override
  public void endWindow()
  {
    HashMap<K, Integer> tuple = null;
    for (Map.Entry<K, MutableInt> e: map.entrySet()) {
      if (tuple == null) {
        tuple = new HashMap<K, Integer>();
      }
      tuple.put(e.getKey(), e.getValue().toInteger());
    }
    if (tuple != null) {
      count.emit(tuple);
    }
    map.clear();
  }
}
