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
import org.apache.apex.malhar.lib.util.HighLow;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.util.UnifierKeyValRange;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;

/**
 *  This operator emits the range for each key at the end of window.
 * <p>
 * <br>
 * <b>StateFull : Yes</b>, values are computed over application window. <br>
 * <b>Partitions : Yes, </b> high/low values are each key is unified at output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>range</b>: emits KeyValPair&lt;K,HighLow&lt;V&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * @displayName Range Key Value
 * @category Math
 * @tags range, number, comparison, key value
 * @since 0.3.3
 */
public class RangeKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{

  /**
   * key/high value map.
   */
  protected HashMap<K, V> high = new HashMap<K, V>();

  /**
   * key/low value map.
   */
  protected HashMap<K, V> low = new HashMap<K, V>();

  /**
   *  Input port that takes a key value pair.
   */
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>()
  {
    /**
     * Process each key and computes new high and low.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      if (!doprocessKey(key) || (tuple.getValue() == null)) {
        return;
      }
      V val = low.get(key);
      V eval = tuple.getValue();
      if ((val == null) || (val.doubleValue() > eval.doubleValue())) {
        low.put(cloneKey(key), eval);
      }

      val = high.get(key);
      if ((val == null) || (val.doubleValue() < eval.doubleValue())) {
        high.put(cloneKey(key), eval);
      }
    }

    @Override
    public StreamCodec<KeyValPair<K, V>> getStreamCodec()
    {
      return getKeyValPairStreamCodec();
    }
  };

  /**
   * Range output port to send out the high low range.
   */
  public final transient DefaultOutputPort<KeyValPair<K, HighLow<V>>> range = new DefaultOutputPort<KeyValPair<K, HighLow<V>>>()
  {
    @Override
    public Unifier<KeyValPair<K, HighLow<V>>> getUnifier()
    {
      return new UnifierKeyValRange<K,V>();
    }
  };

  /**
   * Emits range for each key. If no data is received, no emit is done Clears
   * the internal data before return
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, V> e : high.entrySet()) {
      range.emit(new KeyValPair<K, HighLow<V>>(e.getKey(), new HighLow(e
          .getValue(), low.get(e.getKey()))));
    }
    clearCache();
  }

  public void clearCache()
  {
    high.clear();
    low.clear();
  }
}
