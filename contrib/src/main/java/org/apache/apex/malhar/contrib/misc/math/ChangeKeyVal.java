/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.misc.math;

import java.util.HashMap;

import org.apache.apex.malhar.lib.util.BaseNumberKeyValueOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Operator compares &lt;key,value&gt; pairs arriving at data and base input ports and stores &lt;key,value&gt; pairs arriving at base port in hash map across the windows.
 * <p/>
 * The &lt;key,value&gt; pairs that arrive at data port are compared with base value if the key exists in the hash map.&nbsp;
 * Change value and percentage are emitted on separate ports.
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
 * @displayName Change Key Value
 * @category Math
 * @tags change, key value
 * @since 0.3.3
 * @deprecated
 */
@Deprecated
public class ChangeKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  /**
   * basemap is a stateful field. It is retained across windows
   */
  private HashMap<K, MutableDouble> basemap = new HashMap<K, MutableDouble>();

  /**
   * Input data port that takes key value pairs.
   */
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
        percent.emit(new KeyValPair<K, Double>(cloneKey(key), (cval / bval.doubleValue()) * 100));
      }
    }
  };

  /**
   * Base value input port, stored in base map for comparison.
   */
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
   * Key, Change output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, V>> change = new DefaultOutputPort<KeyValPair<K, V>>();

  /**
   * Key, Percentage Change pair output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Double>> percent = new DefaultOutputPort<KeyValPair<K, Double>>();
}
