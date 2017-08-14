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

import javax.validation.constraints.Min;

import org.apache.apex.malhar.lib.util.BaseNumberValueOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Compares consecutive input data values, emits &lt;value,percent change value&gt; pair on alert output port, if percent change exceeds certain thresh hold value.
 * <p>
 * Operator is StateFull since current value is stored for comparison in next window. <br>
 * This operator can not be partitioned, partitioning will result in inconsistent base value
 * across replicated copies.
 * <br>
 *
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>alert</b>: emits KeyValPair&lt;K,KeyValPair&lt;V,Double&gt;&gt;(1)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>threshold</b>: The threshold of change between consecutive tuples of the
 * same key that triggers an alert tuple<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * @displayName Change Alert
 * @category Rules and Alerts
 * @tags change, key value, numeric, percentage
 * @since 0.3.3
 * @deprecated
 */
@Deprecated
public class ChangeAlert<V extends Number> extends BaseNumberValueOperator<V>
{
  /**
   * Input port that takes in a number.
   */
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
  {
    /**
     * Process each key, compute change or percent, and emit it. If we get 0 as
     * tuple next will be skipped.
     */
    @Override
    public void process(V tuple)
    {
      double tval = tuple.doubleValue();
      if (baseValue == 0) { // Avoid divide by zero, Emit an error tuple?
        baseValue = tval;
        return;
      }
      double change = tval - baseValue;
      double percent = (change / baseValue) * 100;
      if (percent < 0.0) {
        percent = 0.0 - percent;
      }
      if (percent > percentThreshold) {
        KeyValPair<V, Double> kv = new KeyValPair<V, Double>(cloneKey(tuple),
            percent);
        alert.emit(kv);
      }
      baseValue = tval;
    }
  };


  /**
   * Output port which emits a key value pair.
   */
  public final transient DefaultOutputPort<KeyValPair<V, Double>> alert = new DefaultOutputPort<KeyValPair<V, Double>>();

  /**
   * baseValue is a state full field. It is retained across windows
   */
  private double baseValue = 0;
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
