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
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.HighLow;
import com.datatorrent.lib.util.UnifierHashMapRange;

/**
 * <p>
 * Emits the range for each key at the end of window. <br>
 * Application can allow or block keys by setting filter keys and inverse flag. <br>
 * <br>
 * <b>StateFull : Yes</b>, values are computed over application window. <br>
 * <b>Partitions : Yes</b>, values are unified at output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>range</b>: emits HashMap&lt;K,HighLow&lt;V&gt;&gt; each key has two entries; .get(0) gives Max, .get(1) gives Min<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 *
 * @since 0.3.2
 */
public class RangeMap<K, V extends Number> extends BaseNumberKeyValueOperator<K,V>
{
	/**
	 * Application window high value.
	 */
  protected HashMap<K,V> high = new HashMap<K,V>();
  
  /**
   * Application window low value.
   */
  protected HashMap<K,V> low = new HashMap<K,V>();
  
	/**
	 * Input key/value map port.
	 */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Process each key and computes new high and low
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K,V> e: tuple.entrySet()) {
        K key = e.getKey();
        if (!doprocessKey(key) || (e.getValue() == null)) {
          continue;
        }
        V eval = e.getValue();
        V val = low.get(key);
        if ((val == null) || (val.doubleValue() > eval.doubleValue())) {
          low.put(cloneKey(key), eval);
        }

        val = high.get(key);
        if ((val == null) || (val.doubleValue() < eval.doubleValue())) {
          high.put(cloneKey(key), eval);
        }
      }
    }
  };

  /**
   * Output range port.
   */
  @OutputPortFieldAnnotation(name = "range")
  public final transient DefaultOutputPort<HashMap<K, HighLow<V>>> range = new DefaultOutputPort<HashMap<K, HighLow<V>>>()
  {
    @Override
    public Unifier<HashMap<K, HighLow<V>>> getUnifier()
    {
      return new UnifierHashMapRange<K,V>();
    }
  };

  /**
   * Emits range for each key. If no data is received, no emit is done
   * Clears the internal data before return
   */
  @Override
  public void endWindow()
  {
    HashMap<K, HighLow<V>> tuples = new HashMap<K, HighLow<V>>(1);
    for (Map.Entry<K,V> e: high.entrySet()) {
      tuples.put(e.getKey(), new HighLow(e.getValue(), low.get(e.getKey())));
    }
    if (!tuples.isEmpty()) {
      range.emit(tuples);
    }
    clearCache();
  }

  /**
   * Reset high/low values.
   */
  private void clearCache()
  {
    high.clear();
    low.clear();
  }
}
