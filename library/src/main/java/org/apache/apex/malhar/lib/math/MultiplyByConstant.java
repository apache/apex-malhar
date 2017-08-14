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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * Multiplies input tuple (Number) by the value of property "multiplier" and emits the result on respective ports.
 * <p>
 * This operator emits the result as Long on port "longProduct", as Integer on port "integerProduct", as Double on port "doubleProduct", and as Float on port "floatProduct".
 * Output is computed in current window.No state dependency among input tuples
 * This is a pass through operator
 * <br>
 * <b>StateFull : No </b>, output is computed in current window. <br>
 * <b>Partitions : Yes </b>, No state dependency among input tuples. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Number<br>
 * <b>longProduct</b>: emits Long<br>
 * <b>integerProduct</b>: emits Integer<br>
 * <b>doubleProduct</b>: emits Double<br>
 * <b>floatProduct</b>: emits Float<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>multiplier</b>: Number to multiply input tuple with<br>
 * <br>
 * @displayName Multiply By Constant
 * @category Math
 * @tags multiplication, constant, numeric
 * @since 0.3.2
 */
@Stateless
public class MultiplyByConstant extends BaseOperator
{
  /**
   * Input number port.
   */
  public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>()
  {
    @Override
    public void process(Number tuple)
    {
      Long lProduct = null;
      if (longProduct.isConnected()) {
        longProduct.emit(lProduct = multiplier.longValue() * tuple.longValue());
      }

      if (integerProduct.isConnected()) {
        integerProduct.emit(lProduct == null ? (int)(multiplier.longValue() * tuple.longValue()) : lProduct.intValue());
      }

      Double dProduct = null;
      if (doubleProduct.isConnected()) {
        doubleProduct.emit(dProduct = multiplier.doubleValue() * tuple.doubleValue());
      }

      if (floatProduct.isConnected()) {
        floatProduct.emit(dProduct == null ? (float)(multiplier.doubleValue() * tuple.doubleValue()) : dProduct.floatValue());
      }
    }

  };

  /**
   * Long output port.
   */
  public final transient DefaultOutputPort<Long> longProduct = new DefaultOutputPort<Long>();

  /**
   * Integer output port.
   */
  public final transient DefaultOutputPort<Integer> integerProduct = new DefaultOutputPort<Integer>();

  /**
   * Double output port.
   */
  public final transient DefaultOutputPort<Double> doubleProduct = new DefaultOutputPort<Double>();

  /**
   * Float output port.
   */
  public final transient DefaultOutputPort<Float> floatProduct = new DefaultOutputPort<Float>();

  /**
   * @param multiplier the multiplier to set
   */
  public void setMultiplier(Number multiplier)
  {
    this.multiplier = multiplier;
  }

  /**
   *
   */
  public Number getMultiplier()
  {
    return multiplier;
  }

  private Number multiplier;
}
