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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * This operator implements Unifier interface and emits minimum of all values sub-classed from Number for each key at end of window. 
 * <p>
 * <b>StateFull :</b> Yes, min value is computed over application window. <br>
 * <b>Partitions :</b> Yes, min operator is min unifier for output port.
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V extends Number&gt;<br>
 * <b>min</b>: emits HashMap&lt;K,V extends Number&gt;, one entry per key<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * @displayname: Minimum Map
 * @category: math
 * @tags: minimum, hash map, numeric
 * @since 0.3.2
 */
public class MinMap<K, V extends Number> extends BaseNumberKeyValueOperator<K, V> implements Unifier<HashMap<K,V>>
{

  protected HashMap<K, V> low = new HashMap<K, V>();
  
	/**
	 * Input data port that takes a hashmap and updates the value if there is a new minimum.
	 */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>()
  {
    /**
     * For each key, updates the hash if the new value is a new min.
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      MinMap.this.process(tuple);
    }
  };

  /**
   * Unifier process function.
   */
  @Override
  public void process(HashMap<K, V> tuple)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      K key = e.getKey();
      if (!doprocessKey(key) || (e.getValue() == null)) {
        continue;
      }
      V val = low.get(key);
      if (val == null) {
        low.put(cloneKey(key), e.getValue());
      }
      else if (val.doubleValue() > e.getValue().doubleValue()) {
        low.put(key, e.getValue());
      }
    }
  }

  /**
   * Min value output port.
   */
  @OutputPortFieldAnnotation(name = "min")
  public final transient DefaultOutputPort<HashMap<K, V>> min = new DefaultOutputPort<HashMap<K, V>>()
  {
    @Override
    public Unifier<HashMap<K, V>> getUnifier()
    {
      return MinMap.this;
    }
  };

  /**
   * Emits all key,min value pairs.
   * Override getValue() if you have your own class extended from Number.
   * Clears internal data. Node only works in windowed mode.
   */
  @Override
  public void endWindow()
  {
    if (!low.isEmpty()) {
      HashMap<K, V> tuple = new HashMap<K, V>(low.size());
      for (Map.Entry<K, V> e: low.entrySet()) {
        tuple.put(e.getKey(), e.getValue());
      }
      min.emit(tuple);
    }
    low.clear();
  }
}
