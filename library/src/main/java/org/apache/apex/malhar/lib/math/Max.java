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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;

/**
 *<p>
 * This operator implements Unifier interface and emits maximum of all values sub-classed from Number at end of window in the incoming stream. <br>
 * <b>StateFull :</b> Yes, max value is determined during application window, can be more than 1. <br>
 * <b>Partitions : </b>Yes, operator itself is used as unifier at output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>max</b>: emits V extends Number<br>
 * <br>
 * @displayName Maximum
 * @category Math
 * @tags maximum, numeric
 * @since 0.3.2
 */
public class Max<V extends Number> extends BaseNumberValueOperator<V> implements Unifier<V>
{
  /**
   * Input port that takes a number and compares to max and stores the new max.
   */
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
  {
    /**
     * Each tuple is compared to the max and a new max (if so) is stored.
     */
    @Override
    public void process(V tuple)
    {
      Max.this.process(tuple);
    }
  };

  /**
   * Unifier process function.
   */
  @Override
  public void process(V tuple)
  {
    if (!flag) {
      high = tuple;
      flag = true;
    } else if (high.doubleValue() < tuple.doubleValue()) {
      high = tuple;
    }
  }

  /**
   * Max value output port.
   */
  public final transient DefaultOutputPort<V> max = new DefaultOutputPort<V>()
  {
    @Override
    public Unifier<V> getUnifier()
    {
      return Max.this;
    }
  };

  protected V high;
  protected boolean flag = false;

  /**
   * Node only works in windowed mode. Emits the max. Override getValue if tuple type is mutable
   * Clears internal data
   */
  @Override
  public void endWindow()
  {
    if (flag) {
      max.emit(high);
    }
    flag = false;
    high = null;
  }
}
