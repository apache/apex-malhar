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

import javax.validation.constraints.Min;

import org.apache.apex.malhar.lib.util.BaseNumberKeyValueOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Operator compares consecutive values arriving at input port mapped by keys, emits &lt;key,percent change&gt; pair on output alert port if percent change exceeds percentage threshold set in operator.
 * <p>
 * StateFull : Yes, current key/value is stored in operator for comparison in
 * next successive windows. <br>
 * Partition(s): No, base comparison value will be inconsistent across
 * instantiated copies. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>alert</b>: emits KeyValPair&lt;K,KeyValPair&lt;V,Double&gt;&gt;(1)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>threshold</b>: The threshold of change between consecutive tuples of the
 * same key that triggers an alert tuple<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * @displayName Change Alert Key Value
 * @category Rules and Alerts
 * @tags change, key value, numeric, percentage
 * @since 0.3.3
 * @deprecated
 */
@Deprecated
public class ChangeAlertKeyVal<K, V extends Number> extends
    BaseNumberKeyValueOperator<K, V>
{
  /**
   * Base map is a StateFull field. It is retained across windows
   */
  private HashMap<K, MutableDouble> basemap = new HashMap<K, MutableDouble>();

  /**
   * Input data port that takes a key value pair.
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
      double tval = tuple.getValue().doubleValue();
      MutableDouble val = basemap.get(key);
      if (!doprocessKey(key)) {
        return;
      }
      if (val == null) { // Only process keys that are in the basemap
        val = new MutableDouble(tval);
        basemap.put(cloneKey(key), val);
        return;
      }
      double change = tval - val.doubleValue();
      double percent = (change / val.doubleValue()) * 100;
      if (percent < 0.0) {
        percent = 0.0 - percent;
      }
      if (percent > percentThreshold) {
        KeyValPair<V, Double> dmap = new KeyValPair<V, Double>(
            cloneValue(tuple.getValue()), percent);
        KeyValPair<K, KeyValPair<V, Double>> otuple = new KeyValPair<K, KeyValPair<V, Double>>(
            cloneKey(key), dmap);
        alert.emit(otuple);
      }
      val.setValue(tval);
    }
  };

  /**
   * Key,Percent Change output port.
   */
  public final transient DefaultOutputPort<KeyValPair<K, KeyValPair<V, Double>>> alert = new DefaultOutputPort<KeyValPair<K, KeyValPair<V, Double>>>();

  /**
   * Alert thresh hold percentage set by application.
   */
  @Min(1)
  private double percentThreshold = 0.0;

  /**
   * getter function for threshold value
   *
   * @return threshold value
   */
  @Min(1)
  public double getPercentThreshold()
  {
    return percentThreshold;
  }

  /**
   * setter function for threshold value
   */
  public void setPercentThreshold(double d)
  {
    percentThreshold = d;
  }
}
