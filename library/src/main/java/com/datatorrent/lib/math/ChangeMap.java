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

import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.lib.util.BaseNumberKeyValueOperator;

/**
 * <p/>
 * Operator takes input from two ports. Operator stores input arriving base port
 * for comparison across the windows. kay/value map arriving are compared with
 * base map, on per key base. For all existing keys map of key/change and
 * key/percent change values are emitted on separate ports. <br>
 * <p/>
 * <br>
 * StateFull : Yes, base values are stored across windows for comparison. <br>
 * Partitions : Yes, values on the base port are replicated across all partitions. The order of tuples from output port may
 * change.
 * <br>
 * <p/>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>base</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>change</b>: emits HashMap&lt;K,V&gt;(1)<br>
 * <b>percent</b>: emits HashMap&lt;K,Double&gt;(1)<br>
 * <br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 *
 * @since 0.3.3
 */
public class ChangeMap<K, V extends Number> extends
  BaseNumberKeyValueOperator<K, V>
{
  /**
   * basemap is a stateful field. It is retained across windows
   */
  private HashMap<K, MutableDouble> basemap = new HashMap<K, MutableDouble>();

  /**
   * Data key/value map input port.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Process each key, compute change or percent, and emit it.
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K, V> e : tuple.entrySet()) {
        if (!doprocessKey(e.getKey())) {
          continue;
        }
        MutableDouble bval = basemap.get(e.getKey());
        if (bval != null) { // Only process keys that are in the basemap
          double cval = e.getValue().doubleValue() - bval.doubleValue();
          HashMap<K, V> ctuple = new HashMap<K, V>(1);
          ctuple.put(cloneKey(e.getKey()), getValue(cval));
          change.emit(ctuple);
          HashMap<K, Double> ptuple = new HashMap<K, Double>(1);
          ptuple.put(cloneKey(e.getKey()), (cval / bval.doubleValue()) * 100);
          percent.emit(ptuple);
        }
      }
    }
  };

  /**
   * Base key/value map input port, for comparison.
   */
  @InputPortFieldAnnotation(name = "base")
  public final transient DefaultInputPort<Map<K, V>> base = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Process each key to store the value. If same key appears again update
     * with latest value.
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K, V> e : tuple.entrySet()) {
        if (e.getValue().doubleValue() != 0.0) { // Avoid divide by zero, Emit
          // an error tuple?
          MutableDouble val = basemap.get(e.getKey());
          if (val == null) {
            val = new MutableDouble(0.0);
            basemap.put(cloneKey(e.getKey()), val);
          }
          val.setValue(e.getValue().doubleValue());
        }
      }
    }
  };

  /**
   * Output port emits map for key/change.
   */
  @OutputPortFieldAnnotation(name = "change", optional = true)
  public final transient DefaultOutputPort<HashMap<K, V>> change = new DefaultOutputPort<HashMap<K, V>>();

  /**
   * Output port emits map for key/percent change.
   */
  @OutputPortFieldAnnotation(name = "percent", optional = true)
  public final transient DefaultOutputPort<HashMap<K, Double>> percent = new DefaultOutputPort<HashMap<K, Double>>();
}
