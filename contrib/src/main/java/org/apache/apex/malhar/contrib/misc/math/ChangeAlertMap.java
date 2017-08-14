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
import java.util.Map;

import javax.validation.constraints.Min;

import org.apache.apex.malhar.lib.util.BaseNumberKeyValueOperator;
import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Operator stores  &lt;key,value&gt; pair in hash map across the windows for comparison and emits hash map of &lt;key,percent change in value for each key&gt; if percent change
 * exceeds preset threshold.
 * <p>
 *
 * StateFull : Yes, key/value pair in current window are stored for comparison in next window. <br>
 * Partition : No, will yield wrong result, base value won't be consistent across instances. <br>
 *
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>alert</b>: emits HashMap&lt;K,HashMap&lt;V,Double&gt;&gt;(1)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>threshold</b>: The threshold of change between consecutive tuples of the same key that triggers an alert tuple<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * @displayName Change Alert Map
 * @category Rules and Alerts
 * @tags change, key value, numeric, percentage, map
 * @since 0.3.2
 * @deprecated
 */
@Deprecated
public class ChangeAlertMap<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  /**
   * Input data port that takes a map of &lt;key,value&gt;.
   */
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Process each key, compute change or percent, and emits it.
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        MutableDouble val = basemap.get(e.getKey());
        if (!doprocessKey(e.getKey())) {
          continue;
        }
        if (val == null) { // Only process keys that are in the basemap
          val = new MutableDouble(e.getValue().doubleValue());
          basemap.put(cloneKey(e.getKey()), val);
          continue;
        }
        double change = e.getValue().doubleValue() - val.doubleValue();
        double percent = (change / val.doubleValue()) * 100;
        if (percent < 0.0) {
          percent = 0.0 - percent;
        }
        if (percent > percentThreshold) {
          HashMap<V,Double> dmap = new HashMap<V,Double>(1);
          dmap.put(cloneValue(e.getValue()), percent);
          HashMap<K,HashMap<V,Double>> otuple = new HashMap<K,HashMap<V,Double>>(1);
          otuple.put(cloneKey(e.getKey()), dmap);
          alert.emit(otuple);
        }
        val.setValue(e.getValue().doubleValue());
      }
    }
  };

  // Default "pass through" unifier works as tuple is emitted as pass through
  /**
   * Output port which emits a hashmap of key, percentage change.
   */
  public final transient DefaultOutputPort<HashMap<K, HashMap<V,Double>>> alert = new DefaultOutputPort<HashMap<K, HashMap<V,Double>>>();

  /**
   * basemap is a statefull field. It is retained across windows
   */
  private HashMap<K,MutableDouble> basemap = new HashMap<K,MutableDouble>();
  @Min(1)
  private double percentThreshold = 0.0;

  /**
   * getter function for threshold value
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
