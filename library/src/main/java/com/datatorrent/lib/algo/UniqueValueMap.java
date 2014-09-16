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
import java.util.HashSet;
import java.util.Map;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseKeyOperator;
import com.datatorrent.lib.util.UnifierHashMap;

/**
 * <p>
 * This operator counts the number of unique values corresponding to a key within a window.&nbsp;
 * At the end of each window each key/unique count pair is emitted.
 * <p>
 * Count unique occurrences of vals for every key within a window, and emits Key,Integer pairs tuple.<p>
 * This is an end of window operator. It uses sticky key partition and default unifier<br>
 * <br>
 * <b>StateFull : Yes, </b> Tuple are aggregated across application window(s). <br>
 * <b>Partitions : Yes, </b> Top values are unified on output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V&gt;<br>
 * <b>count</b>: emits HashMap&lt;K,Integer&gt;<br>
 * <br>
 *
 * @displayName Count Unique Values Per Key (Map)
 * @category algorithm
 * @tags count, keyval
 *
 * @since 0.3.2
 */
@OperatorAnnotation(partitionable = true)
public class UniqueValueMap<K> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, ? extends Object>> data = new DefaultInputPort<Map<K, ? extends Object>>()
  {
    /**
     * Reference counts tuples
     */
    @Override
    public void process(Map<K, ? extends Object> tuple)
    {
      for (Map.Entry<K, ? extends Object> e: tuple.entrySet()) {
        HashSet<Object> vals = map.get(e.getKey());
        if (vals == null) {
          vals = new HashSet<Object>();
          map.put(cloneKey(e.getKey()), vals);
        }
        vals.add(e.getValue());
      }
    }
  };
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>()
  {
    @Override
    public Unifier<HashMap<K, Integer>> getUnifier()
    {
      return new UnifierHashMap<K, Integer>();
    }
  };

  /**
   * Bucket counting mechanism.
   */
  protected HashMap<K, HashSet<Object>> map = new HashMap<K, HashSet<Object>>();


  /**
   * Emits one HashMap as tuple
   */
  @Override
  public void endWindow()
  {
    if (!map.isEmpty()) {
      HashMap<K, Integer> tuple = new HashMap<K, Integer>(map.size());
      for (Map.Entry<K, HashSet<Object>> e: map.entrySet()) {
        tuple.put(e.getKey(), e.getValue().size());
      }
      count.emit(tuple);
      clearCache();
    }
  }

  public void clearCache()
  {
    map.clear();
  }
}
