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
import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;

/**
 *
 * This operator adds all values for each key in "numerator" and "denominator", and emits the margin for each key at the end of window.
 * <p>
 * <br>
 * Margin Formula used by this operator: 1 - numerator/denominator.
 * The values are added for each key within the window and for each stream.<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>numerator</b>: expects KeyValPair&lt;K,V&gt;<br>
 * <b>denominator</b>: expects KeyValPair&lt;K,V&gt;<br>
 * <b>margin</b>: emits HashMap&lt;K,Double&gt;, one entry per key per window<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * @displayName Margin Key Value
 * @category Math
 * @tags sum, division, numeric, key value
 * @since 0.3.3
 */
public class MarginKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
        /**
   * Numerator input port that takes a key value pair.
   */
  public final transient DefaultInputPort<KeyValPair<K, V>> numerator = new DefaultInputPort<KeyValPair<K, V>>()
  {
    /**
     * Adds tuple to the numerator hash.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      addTuple(tuple, numerators);
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
   * Denominator input port that takes a key value pair.
   */
  public final transient DefaultInputPort<KeyValPair<K, V>> denominator = new DefaultInputPort<KeyValPair<K, V>>()
  {
    /**
     * Adds tuple to the denominator hash.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      addTuple(tuple, denominators);
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
   * Adds the value for each key.
   *
   * @param tuple
   * @param map
   */
  public void addTuple(KeyValPair<K, V> tuple, Map<K, MutableDouble> map)
  {
    K key = tuple.getKey();
    if (!doprocessKey(key) || (tuple.getValue() == null)) {
      return;
    }
    MutableDouble val = map.get(key);
    if (val == null) {
      val = new MutableDouble(0.0);
      map.put(cloneKey(key), val);
    }
    val.add(tuple.getValue().doubleValue());
  }

        /**
   * Output margin port that emits Key Value pairs.
   */
  public final transient DefaultOutputPort<KeyValPair<K, V>> margin = new DefaultOutputPort<KeyValPair<K, V>>();

  protected HashMap<K, MutableDouble> numerators = new HashMap<K, MutableDouble>();
  protected HashMap<K, MutableDouble> denominators = new HashMap<K, MutableDouble>();
  protected boolean percent = false;

  /**
   * getter function for percent
   *
   * @return percent
   */
  public boolean getPercent()
  {
    return percent;
  }

  /**
   * setter function for percent
   *
   * @param val
   *          sets percent
   */
  public void setPercent(boolean val)
  {
    percent = val;
  }

  /**
   * Generates tuples for each key and emits them. Only keys that are in the
   * denominator are iterated on If the key is only in the numerator, it gets
   * ignored (cannot do divide by 0) Clears internal data
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void endWindow()
  {
    Double val;
    for (Map.Entry<K, MutableDouble> e : denominators.entrySet()) {
      K key = e.getKey();
      MutableDouble nval = numerators.get(key);
      if (nval == null) {
        nval = new MutableDouble(0.0);
      } else {
        numerators.remove(key); // so that all left over keys can be reported
      }
      if (percent) {
        val = (1 - nval.doubleValue() / e.getValue().doubleValue()) * 100;
      } else {
        val = 1 - nval.doubleValue() / e.getValue().doubleValue();
      }

      margin.emit(new KeyValPair(key, getValue(val.doubleValue())));
    }

    numerators.clear();
    denominators.clear();
  }
}
