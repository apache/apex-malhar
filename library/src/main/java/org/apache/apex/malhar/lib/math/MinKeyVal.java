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
package org.apache.apex.malhar.lib.math;

import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.util.BaseNumberKeyValueOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;

/**
 *
 * This operator emits minimum of all values sub-classed from Number for each key in KeyValPair at end of window. <p>
 * <br>
 *
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>min</b>: emits KeyValPair&lt;K,V extends Number&gt;, one entry per key<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * @displayName Minimum Key Value
 * @category Math
 * @tags minimum, numeric, key value
 * @since 0.3.2
 */
public class MinKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  /**
   * Input port which takes a key vaue pair and updates the value for each key if there is a new min.
   */
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>()
  {
    /**
     * For each key, updates the hash if the new value is a new min.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      V tval = tuple.getValue();
      if (!doprocessKey(key) || (tval == null)) {
        return;
      }
      V val = mins.get(key);
      if (val == null) {
        mins.put(cloneKey(key), tval);
      } else if (val.doubleValue() > tval.doubleValue()) {
        mins.put(key, tval);
      }
    }

    /**
     * Set StreamCodec used for partitioning.
     */
    @Override
    public StreamCodec<KeyValPair<K, V>> getStreamCodec()
    {
      return getKeyValPairStreamCodec();
    }
  };

  /**
   * Min value output port.
   */
  public final transient DefaultOutputPort<KeyValPair<K, V>> min = new DefaultOutputPort<KeyValPair<K, V>>();
  protected HashMap<K, V> mins = new HashMap<K, V>();

  /**
   * Emits all key,min value pairs.
   * Clears internal data. Node only works in windowed mode.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void endWindow()
  {
    if (!mins.isEmpty()) {
      for (Map.Entry<K, V> e: mins.entrySet()) {
        min.emit(new KeyValPair(e.getKey(), e.getValue()));
      }
      mins.clear();
    }
  }
}
