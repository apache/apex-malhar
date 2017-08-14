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

import org.apache.apex.malhar.lib.util.BaseNumberValueOperator;
import org.apache.apex.malhar.lib.util.HighLow;
import org.apache.apex.malhar.lib.util.UnifierRange;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This operator emits the range of values at the end of window.
 * <p>
 * <br>
 * <b>StateFull : Yes</b>, values are computed over application time window. <br>
 * <b>Partitions : Yes </b>, High/Low values are unified on output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>range</b>: emits HighLow&lt;V&gt;<br>
 * <br>
 * <br>
 * @displayName Range
 * @category Math
 * @tags range, numeric , comparison
 * @since 0.3.3
 */
public class Range<V extends Number> extends BaseNumberValueOperator<V>
{
  /**
   * Highest value on input port.
   */
  protected V high = null;

  /**
   * Lowest value on input port.
   */
  protected V low = null;

  /**
   * Input data port.
   */
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
  {
    /**
     * Process each tuple to compute new high and low
     */
    @Override
    public void process(V tuple)
    {
      if ((low == null) || (low.doubleValue() > tuple.doubleValue())) {
        low = tuple;
      }

      if ((high == null) || (high.doubleValue() < tuple.doubleValue())) {
        high = tuple;
      }
    }
  };

  /**
   * Output range port, which emits high low unifier operator.
   */
  public final transient DefaultOutputPort<HighLow<V>> range = new DefaultOutputPort<HighLow<V>>()
  {
    @Override
    public Unifier<HighLow<V>> getUnifier()
    {
      return new UnifierRange<V>();
    }
  };

  /**
   * Emits the range. If no tuple was received in the window, no emit is done
   * Clears the internal data before return
   */
  @Override
  public void endWindow()
  {
    if ((low != null) && (high != null)) {
      HighLow tuple = new HighLow(getValue(high.doubleValue()),
          getValue(low.doubleValue()));
      range.emit(tuple);
    }
    high = null;
    low = null;
  }
}
