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
package com.datatorrent.lib.math;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;

/**
 *
 * This operator implements Unifier interface and emits maximum of all values sub-classed from Number for each key in HashMap at end of window.
 * <p>
 * <br>
 *  <b>StateFull : </b>Yes, key/value max is determined over application window, can be > 1. <br>
 *  <b>Partitions : </b>Yes, operator is max unifier operator on output port.
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V extends Number&gt;<br>
 * <b>max</b>: emits HashMap&lt;K,V extends Number&gt;, one entry per key<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * @displayName Maximum Map
 * @category Math
 * @tags maximum, numeric, hash map, round robin partition
 * @since 0.3.2
 */
public class MaxMap<K, V extends Number> extends BaseNumberKeyValueOperator<K,V> implements Unifier<HashMap<K,V>>
{
  /**
   * Input port that takes a hashmap and updates the value for each key if there is a new max.
   */
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>()
  {
    /**
     * For each key, updates the hash if the new value is a new max.
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      MaxMap.this.process(tuple);
    }
  };

  @Override
  public void process(HashMap<K, V> tuple)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      K key = e.getKey();
      if (!doprocessKey(key) || (e.getValue() == null)) {
        continue;
      }
      V val = high.get(key);
      if (val == null) {
        val = e.getValue();
        high.put(cloneKey(key), val);
      }
      if (val.doubleValue() < e.getValue().doubleValue()) {
        val = e.getValue();
        high.put(key, val);
      }
    }
  }
 
  
  /**
   * Max value output port.
   */
  public final transient DefaultOutputPort<HashMap<K,V>> max = new DefaultOutputPort<HashMap<K,V>>()
  {
    @Override
    public Unifier<HashMap<K, V>> getUnifier()
    {
      return MaxMap.this;
    }
  };

  protected HashMap<K,V> high = new HashMap<K,V>();

  /**
   * Emits all key,max value pairs.
   * Override getValue() if you have your own class extended from Number.
   * Clears internal data. Node only works in windowed mode.
   */
  @Override
  public void endWindow()
  {
    if (!high.isEmpty()) {
      HashMap<K, V> tuple = new HashMap<K, V>(high.size());
      for (Map.Entry<K,V> e: high.entrySet()) {
        tuple.put(e.getKey(), e.getValue());
      }
      max.emit(tuple);
      high.clear();
    }
  }
}
