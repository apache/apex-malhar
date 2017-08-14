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
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This operator sums the division of numerator and denominator value arriving at input ports.
 * <p>
 * <br>
 * Margin Formula used by this operator: 1 - numerator/denominator.<br>
 * If percent flag is set then margin is emitted as percentage.
 * <br>
 * StateFull : Yes, numerator and denominator are summed for application
 * windows. <br>
 * Partitions : No, will yield wrong margin result, no unifier on output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>numerator</b>: expects V extends Number<br>
 * <b>denominator</b>: expects V extends Number<br>
 * <b>margin</b>: emits Double<br>
 * <br>
 * <b>Properties:<b>
 * <br>
 * <b>percent: </b>  output margin as percentage value.
 * @displayName Margin
 * @category Math
 * @tags sum, division, numeric
 * @since 0.3.3
 */
@OperatorAnnotation(partitionable = false)
public class Margin<V extends Number> extends BaseNumberValueOperator<V>
{
  /**
   * Sum of numerator values.
   */
  protected double nval = 0.0;

  /**
   * sum of denominator values.
   */
  protected double dval = 0.0;

  /**
   * Flag to output margin as percentage.
   */
  protected boolean percent = false;

  /**
   * Numerator input port.
   */
  public final transient DefaultInputPort<V> numerator = new DefaultInputPort<V>()
  {
    /**
     * Adds to the numerator value
     */
    @Override
    public void process(V tuple)
    {
      nval += tuple.doubleValue();
    }
  };

  /**
   * Denominator input port.
   */
  public final transient DefaultInputPort<V> denominator = new DefaultInputPort<V>()
  {
    /**
     * Adds to the denominator value
     */
    @Override
    public void process(V tuple)
    {
      dval += tuple.doubleValue();
    }
  };

  /**
   * Output margin port.
   */
  public final transient DefaultOutputPort<V> margin = new DefaultOutputPort<V>();

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
   * Generates tuple emits it as long as denomitor is not 0 Clears internal data
   */
  @Override
  public void endWindow()
  {
    if (dval == 0) {
      return;
    }
    double val = 1 - (nval / dval);
    if (percent) {
      val = val * 100;
    }
    margin.emit(getValue(val));
    nval = 0.0;
    dval = 0.0;
  }
}
