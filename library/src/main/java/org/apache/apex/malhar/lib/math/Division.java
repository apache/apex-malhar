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

import java.util.ArrayList;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * This operator does division metric on consecutive tuples on ports.
 * <p>
 * The operator is idempotent as the division is done in order, i.e. the first number on denominator port would divide the first number on the numerator port.<p>
 * This is a pass through operator<br>
 * <br>
 * StateFull : No, quotient is calculated in current window.
 * Partitions : Yes, since each denominator and numerator are treated indiviually.
 * <p>
 * <b>Ports</b>:<br>
 * <b>numerator</b>: expects Number<br>
 * <b>denominator</b>: expects Number<br>
 * <b>longQuotient</b>: emits Long<br>
 * <b>integerQuotient</b>: emits Integer<br>
 * <b>doubleQuotient</b>: emits Double<br>
 * <b>floatQuotient</b>: emits Float<br>
 * <b>longRemainder</b>: emits Long<br>
 * <b>integerRemainder</b>: emits Integer<br>
 * <b>doubleRemainder</b>: emits Double<br>
 * <b>floatRemainder</b>: emits Float<br>
 * <b>errordata</b>: emits String<br>
 * <br>
 * @displayName Division
 * @category Math
 * @tags division, number, idempotent
 * @since 0.3.2
 */
public class Division extends BaseOperator
{
  /**
   * Array to store numerator inputs during window.
   */
  private ArrayList<Number> numer = new ArrayList<Number>();

  /**
   * Array to store denominator input during window.
   */
  private ArrayList<Number> denom = new ArrayList<Number>();

  /**
   * Number of pair processed in current window.
   */
  private int index = 0;

  /**
   * Numerator input port.
   */
  public final transient DefaultInputPort<Number> numerator = new DefaultInputPort<Number>()
  {
    @Override
    public void process(Number tuple)
    {
      numer.add(tuple);
      if (denom.size() > index) {
        int loc = denom.size();
        if (loc > numer.size()) {
          loc = numer.size();
        }
        emit(numer.get(loc - 1), denom.get(loc - 1));
        index++;
      }
    }
  };

  /**
   * Denominator input port.
   */
  public final transient DefaultInputPort<Number> denominator = new DefaultInputPort<Number>()
  {
    @Override
    public void process(Number tuple)
    {
      if (tuple.doubleValue() == 0.0) {
        errordata.emit("Error(0.0)");
        return;
      }
      denom.add(tuple);
      if (numer.size() > index) {
        int loc = denom.size();
        if (loc > numer.size()) {
          loc = numer.size();
        }
        emit(numer.get(loc - 1), denom.get(loc - 1));
        index++;
      }
    }
  };

  /**
   * Long quotient output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Long> longQuotient = new DefaultOutputPort<Long>();

  /**
   * Integer quotient output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Integer> integerQuotient = new DefaultOutputPort<Integer>();

  /**
   * Double quotient output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Double> doubleQuotient = new DefaultOutputPort<Double>();

  /**
   * Float quotient output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Float> floatQuotient = new DefaultOutputPort<Float>();

  /**
   * Long remainder output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Long> longRemainder = new DefaultOutputPort<Long>();

  /**
   * Integer remainder output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Integer> integerRemainder = new DefaultOutputPort<Integer>();

  /**
   * Double remainder output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Double> doubleRemainder = new DefaultOutputPort<Double>();

  /**
   * Float remainder output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Float> floatRemainder = new DefaultOutputPort<Float>();

  /**
   * Error data output port that emits a string.
   */
  @OutputPortFieldAnnotation(error = true)
  public final transient DefaultOutputPort<String> errordata = new DefaultOutputPort<String>();


  public void emit(Number numer, Number denom)
  {
    Long lQuotient = null;
    Double dQuotient = null;
    Long lRemainder = null;
    Double dRemainder = null;

    if (longQuotient.isConnected()) {
      longQuotient.emit(lQuotient = numer.longValue() / denom.longValue());
    }

    if (longRemainder.isConnected()) {
      longRemainder.emit(lRemainder = numer.longValue() % denom.longValue());
    }

    if (integerQuotient.isConnected()) {
      integerQuotient.emit(lQuotient == null ? (int)(numer.longValue() % denom.longValue()) : lQuotient.intValue());
    }

    if (integerRemainder.isConnected()) {
      integerRemainder.emit(lRemainder == null ? (int)(numer.longValue() % denom.longValue()) : lRemainder.intValue());
    }

    if (doubleQuotient.isConnected()) {
      doubleQuotient.emit(dQuotient = numer.doubleValue() / denom.doubleValue());
    }

    if (doubleRemainder.isConnected()) {
      doubleRemainder.emit(dRemainder = numer.doubleValue() % denom.doubleValue());
    }

    if (floatQuotient.isConnected()) {
      floatQuotient.emit(dQuotient == null ? (float)(numer.doubleValue() / denom.doubleValue()) : dQuotient.floatValue());
    }

    if (floatRemainder.isConnected()) {
      floatRemainder.emit(dRemainder == null ? (float)(numer.doubleValue() % denom.doubleValue()) : dRemainder.floatValue());
    }
  }

  @Override
  public void endWindow()
  {
    numer.clear();
    denom.clear();
    index = 0;
  }
}
