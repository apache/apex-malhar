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
package org.apache.apex.malhar.lib.statistics;

import org.apache.apex.malhar.lib.util.BaseNumberValueOperator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * An implementation of BaseOperator that computes weighted mean of incoming data. <br>
 * <p>
 * <b>Input Port(s) : </b><br>
 * <b>data : </b> Data values input port. <br>
 * <b>weight : </b> Current input data weight. <br>
 * <br>
 * <b>Output Port(s) : </b> <br>
 * <b>mean : </b>Weighted mean output port. <br>
 * <br>
 * <b>StateFull : Yes</b>, value are aggregated over application window. <br>
 * <b>Partitions : No</b>, no will yeild wrong results. <br>
 * <br>
 * @displayName Weighted Mean
 * @category Stats and Aggregations
 * @tags numeric, math, calculation, sum, count, mean operator, average
 * @since 0.3.4
 */
@OperatorAnnotation(partitionable = false)
public class WeightedMeanOperator<V extends Number>  extends BaseNumberValueOperator<V>
{
  // aggregate weighted sum
  private double weightedSum;

  // aggregate weighted count
  private double weightedCount;

  // current input weight
  private double currentWeight;

  /**
   * Input data port that takes a number.
   */
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
  {
    /**
     * Computes sum and count with each tuple
     */
    @Override
    public void process(V tuple)
    {
      weightedSum += currentWeight * tuple.doubleValue();
      weightedCount += currentWeight;
    }
  };

  /**
   * Input weight port that takes a number.
   */
  public final transient DefaultInputPort<V> weight = new DefaultInputPort<V>()
  {
    /**
     * Computes sum and count with each tuple
     */
    @Override
    public void process(V tuple)
    {
      if (tuple.doubleValue() != 0.0) {
        currentWeight = tuple.doubleValue();
      }
    }
  };

  /**
   * Output port that emits weighted mean.
   */
  public final transient DefaultOutputPort<V> mean = new DefaultOutputPort<V>();

  @Override
  public void setup(OperatorContext arg0)
  {
    currentWeight = 1.0;
  }

  @Override
  public void endWindow()
  {
    if (weightedCount != 0.0) {
      mean.emit(getAverage());
    }
    weightedSum = 0.0;
    weightedCount = 0.0;
  }

  /**
   * Calculate average based on number type.
   */
  @SuppressWarnings("unchecked")
  public V getAverage()
  {
    if (weightedSum == 0) {
      return null;
    }
    V num = getValue(weightedSum);
    Number val;
    switch (getType()) {
      case DOUBLE:
        val = num.doubleValue() / weightedCount;
        break;
      case INTEGER:
        val = (int)(num.intValue() / weightedCount);
        break;
      case FLOAT:
        val = new Float(num.floatValue() / weightedCount);
        break;
      case LONG:
        val = (long)(num.longValue() / weightedCount);
        break;
      case SHORT:
        val = (short)(num.shortValue() / weightedCount);
        break;
      default:
        val = num.doubleValue() / weightedCount;
        break;
    }
    return (V)val;
  }
}
