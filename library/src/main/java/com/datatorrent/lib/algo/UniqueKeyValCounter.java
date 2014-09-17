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
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseUniqueKeyValueCounter;
import com.datatorrent.lib.util.UnifierHashMapSumKeys;

/**
 * This operator counts the unique occurrences of key value pairs within each window,
 * and emits a map from key value pairs to counts at the end of each window.
 * <p>
 * Count unique occurrences of key,val pairs within a window, and emits one HashMap tuple.
 * </p>
 * <p>
 * This is an end of window operator<br>
 * <br>
 * <b>StateFull : yes, </b> Tuples are aggregated over application window(s). <br>
 * <b>Partitions : Yes, </b> Unique count is unified at output port. <br>
 * <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>count</b>: emits HashMap&lt;HashMap&lt;K,V&gt;(1),Integer&gt;(1)<br>
 * <br>
 * </p>
 *
 * @displayName Count Unique Keyval Pairs
 * @category algorithm
 * @tags count, key value
 *
 * @since 0.3.2
 */

@OperatorAnnotation(partitionable = true)
public class UniqueKeyValCounter<K,V> extends BaseUniqueKeyValueCounter<K,V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>()
  {
    /**
     * Reference counts tuples
     */
    @Override
    public void process(HashMap<K,V> tuple)
    {
      for (Map.Entry<K,V> e: tuple.entrySet()) {
        processTuple(e.getKey(), e.getValue());
      }
    }
  };
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<HashMap<HashMap<K,V>, Integer>> count = new DefaultOutputPort<HashMap<HashMap<K,V>, Integer>>()
  {
    @Override
    public Unifier<HashMap<HashMap<K,V>, Integer>> getUnifier()
    {
      return new UnifierHashMapSumKeys<HashMap<K,V> ,Integer>();
    }
  };

  /**
   * Emits one HashMap as tuple
   */
  @Override
  public void endWindow()
  {
    HashMap<HashMap<K,V>, Integer> tuple = null;
    for (Map.Entry<HashMap<K,V>, MutableInt> e: map.entrySet()) {
      if (tuple == null) {
        tuple = new HashMap<HashMap<K,V>, Integer>();
      }
      tuple.put(e.getKey(), e.getValue().toInteger());
    }
    if (tuple != null) {
      count.emit(tuple);
    }
    map.clear();
  }
}
