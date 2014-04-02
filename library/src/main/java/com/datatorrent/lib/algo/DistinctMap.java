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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseKeyValueOperator;
import com.datatorrent.lib.util.UnifierHashMap;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Computes and emits distinct key,val pairs (i.e drops duplicates)<p>
 * This is a pass through operator<br>
 * <br>
 * This module is same as a "FirstOf" metric on any key,val pair. At end of window all data is flushed.<br>
 * <br>
 * <b>StateFull : Yes, </b> tuple are compare across application window(s). <br>
 * <b>Partitions : Yes, </b> distinct output is unified by unifier hash map operator. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input data port expects Map&lt;K,V&gt;<br>
 * <b>distinct</b>: Output data port, emits HashMap&lt;K,V&gt;(1)<br>
 * <br>
 *
 * @since 0.3.2
 */
public class DistinctMap<K, V> extends BaseKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Process HashMap<K,V> tuple on input port data, and emits if match not found. Updates the cache
     * with new key,val pair
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        HashMap<V, Object> vals = mapkeyval.get(e.getKey());
        if ((vals == null) || !vals.containsKey(e.getValue())) {
          HashMap<K, V> otuple = new HashMap<K, V>(1);
          otuple.put(cloneKey(e.getKey()), cloneValue(e.getValue()));
          distinct.emit(otuple);
          if (vals == null) {
            vals = new HashMap<V, Object>();
            mapkeyval.put(cloneKey(e.getKey()), vals);
          }
          vals.put(cloneValue(e.getValue()), null);
        }
      }
    }
  };
  @OutputPortFieldAnnotation(name = "distinct")
  public final transient DefaultOutputPort<HashMap<K, V>> distinct = new DefaultOutputPort<HashMap<K, V>>()
  {
    @Override
    public Unifier<HashMap<K, V>> getUnifier()
    {
      return new UnifierHashMap<K, V>();
    }
  };


  protected HashMap<K, HashMap<V, Object>> mapkeyval = new HashMap<K, HashMap<V, Object>>();

  /**
   * Clears the cache/hash
   */
  @Override
  public void endWindow()
  {
    mapkeyval.clear();
  }
}
