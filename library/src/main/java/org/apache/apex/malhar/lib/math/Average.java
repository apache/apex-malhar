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
import com.datatorrent.common.util.Pair;

/**
 *
 * Emits the average of values at the end of window.
 * <p>
 * This is an end window operator. <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>average</b>: emits V extends Number<br>
 * <br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * @displayName Average
 * @category Math
 * @tags average, numeric, end window
 * @since 0.3.3
 */
public class Average<V extends Number> extends BaseNumberValueOperator<V>
{
  /**
   * Input port that takes a number.
   */
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
  {
    /**
     * Computes sum and count with each tuple
     */
    @Override
    public void process(V tuple)
    {
      sums += tuple.doubleValue();
      counts++;
    }
  };

  /**
   * Output port that emits average as a number.
   */
  public final transient DefaultOutputPort<Pair<V,Long>> average = new DefaultOutputPort<Pair<V, Long>>()
  {
    @Override
    public Unifier<Pair<V, Long>> getUnifier()
    {
      return new AvgUnifier<V>();
    }
  };

  protected double sums = 0;
  protected long counts = 0;

  /**
   * Emit average.
   */
  @Override
  public void endWindow()
  {
    // May want to send out only if count != 0

    if (counts != 0) {
      Pair<V,Long> pair = new Pair<>(getAverage(),counts);
      average.emit(pair);
    }

    sums = 0;
    counts = 0;
  }

  public static class AvgUnifier<V extends Number> extends Average<V> implements Unifier<Pair<V, Long>>
  {

    @Override
    public void process(Pair<V, Long> pair)
    {
      sums += pair.getFirst().doubleValue() * pair.getSecond();
      counts += pair.getSecond();
    }
  }

  /**
   * Calculate average based on number type.
   */
  @SuppressWarnings("unchecked")
  public V getAverage()
  {
    if (counts == 0) {
      return null;
    }
    V num = getValue(sums);
    Number val;
    switch (getType()) {
      case DOUBLE:
        val = new Double(num.doubleValue() / counts);
        break;
      case INTEGER:
        int icount = (int)(num.intValue() / counts);
        val = new Integer(icount);
        break;
      case FLOAT:
        val = new Float(num.floatValue() / counts);
        break;
      case LONG:
        val = new Long(num.longValue() / counts);
        break;
      case SHORT:
        short scount = (short)(num.shortValue() / counts);
        val = new Short(scount);
        break;
      default:
        val = new Double(num.doubleValue() / counts);
        break;
    }
    return (V)val;
  }
}
