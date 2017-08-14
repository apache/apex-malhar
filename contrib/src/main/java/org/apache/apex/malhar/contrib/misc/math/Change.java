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

import org.apache.apex.malhar.lib.util.BaseNumberValueOperator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Operator compares data values arriving on input port with base value input operator.
 *
 * <p>
 * Arriving base value is stored in operator for comparison, old base value is overwritten.&nbsp;
 * This emits &lt;change in value,percentage change&gt;.
 * Operator expects values arriving on data input port and base value input operator.
 * Change in value and percentage change in values are emitted on separate ports.<br>
 * This operator can not be partitioned, since copies won't get consecutive operators. <br>
 * This is StateFull operator, tuples that arrive on base port are kept in
 * cache forever.<br>
 * <br>
 * <b>Input Ports</b>:<br>
 * <b>data</b>: expects V extends Number, Data values<br>
 * <b>base</b>: expects V extends Number, Base Value stored for comparison<br>
 *
 * <b>Output Ports</b>:<br>
 * <b>change</b>: emits V extends Number,  Diff from base value<br>
 * <b>percent</b>: emits Doubl, percent change in value compared to base value.<br>
 * <br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 *
 * <br>
 * @displayName Change
 * @category Math
 * @tags change, key value, numeric, percentage
 * @since 0.3.3
 * @deprecated
 */
@Deprecated
public class Change<V extends Number> extends BaseNumberValueOperator<V>
{
        /**
   * Input data port that takes a number.
   */
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
  {
    /**
     * Process each key, compute change or percent, and emit it.
     */
    @Override
    public void process(V tuple)
    {
      if (baseValue != 0) { // Avoid divide by zero, Emit an error tuple?
        double cval = tuple.doubleValue() - baseValue;
        change.emit(getValue(cval));
        percent.emit((cval / baseValue) * 100);
      }
    }
  };

        /**
   * Input port that takes a number&nbsp; It stores the value for base comparison.
   */
  public final transient DefaultInputPort<V> base = new DefaultInputPort<V>()
  {
    /**
     * Process each key to store the value. If same key appears again update
     * with latest value.
     */
    @Override
    public void process(V tuple)
    {
      if (tuple.doubleValue() != 0.0) { // Avoid divide by zero, Emit an error
                                        // tuple?
        baseValue = tuple.doubleValue();
      }
    }
  };

  /**
   * Output port that emits change in value compared to base value.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<V> change = new DefaultOutputPort<V>();

  /**
   * Output port that emits percent change in data value compared to base value.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Double> percent = new DefaultOutputPort<Double>();

  /**
   * baseValue is a state full field. It is retained across windows.
   */
  private double baseValue = 0;
}
