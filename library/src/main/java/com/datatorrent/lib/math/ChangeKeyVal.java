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

import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;

/**
 * <p/>
 * <br>Operator takes input from two ports - data and base. It stores kay/value pairs arriving at base
 * port in <code>basemap</code> across the windows.</br>
 * <br>The key/value pairs that arrive at data port are compared with base value if the key exists in the <code>basemap</code>.</br>
 * <br>Change value/percent are emitted on separate ports.</br>
 * <p/>
 * StateFull : Yes, base map values are stored across windows. <br>
 * Partitions : Yes, values on the base port are replicated across all partitions. However the order of tuples on the
 * output stream may change.
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>base</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>change</b>: emits KeyValPair&lt;K,V&gt;(1)<br>
 * <b>percent</b>: emits KeyValPair&lt;K,Double&gt;(1)<br>
 * <br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 *
 * @since 0.3.3
 */
public class ChangeKeyVal<K, V extends Number> extends
  BaseNumberKeyValueOperator<K, V>
{
  /**
   * basemap is a stateful field. It is retained across windows
   */
  private HashMap<K, MutableDouble> basemap = new HashMap<K, MutableDouble>();

  /**
   * Data tuples input port.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>()
  {
    /**
     * Process each key, compute change or percent, and emit it.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      if (!doprocessKey(key)) {
        return;
      }
      MutableDouble bval = basemap.get(key);
      if (bval != null) { // Only process keys that are in the basemap
        double cval = tuple.getValue().doubleValue() - bval.doubleValue();
        change.emit(new KeyValPair<K, V>(cloneKey(key), getValue(cval)));
        percent.emit(new KeyValPair<K, Double>(cloneKey(key), (cval / bval
          .doubleValue()) * 100));
      }
    }
  };

  /**
   * Base value port, stored in base map for comparison.
   */
  @InputPortFieldAnnotation(name = "base")
  public final transient DefaultInputPort<KeyValPair<K, V>> base = new DefaultInputPort<KeyValPair<K, V>>()
  {
    /**
     * Process each key to store the value. If same key appears again update
     * with latest value.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      if (tuple.getValue().doubleValue() != 0.0) { // Avoid divide by zero, Emit
        // an error tuple?
        MutableDouble val = basemap.get(tuple.getKey());
        if (val == null) {
          val = new MutableDouble(0.0);
          basemap.put(cloneKey(tuple.getKey()), val);
        }
        val.setValue(tuple.getValue().doubleValue());
      }
    }
  };

  /**
   * Key/Change output port.
   */
  @OutputPortFieldAnnotation(name = "change", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, V>> change = new DefaultOutputPort<KeyValPair<K, V>>();

  /**
   * key/percent change pair output port.
   */
  @OutputPortFieldAnnotation(name = "percent", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Double>> percent = new DefaultOutputPort<KeyValPair<K, Double>>();
}
