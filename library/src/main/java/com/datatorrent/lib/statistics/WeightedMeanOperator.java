/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.statistics;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.BaseNumberValueOperator;

/**
 * This operator computes weighted mean of incoming data. <br>
 * <br>
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
 *
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
   * Input data port.
   */
  @InputPortFieldAnnotation(name = "data")
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
   * Input weight port.
   */
  @InputPortFieldAnnotation(name = "weight")
  public final transient DefaultInputPort<V> weight = new DefaultInputPort<V>()
  {
    /**
     * Computes sum and count with each tuple
     */
    @Override
    public void process(V tuple)
    {
      if (tuple.doubleValue() != 0.0) currentWeight = tuple.doubleValue();
    }
  };
  
  /**
   * Output port
   */
  @OutputPortFieldAnnotation(name = "mean")
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
        val = new Double(num.doubleValue() / weightedCount);
        break;
      case INTEGER:
        int icount = (int) (num.intValue() / weightedCount);
        val = new Integer(icount);
        break;
      case FLOAT:
        val = new Float(num.floatValue() / weightedCount);
        break;
      case LONG:
        val = new Long((long) (num.longValue() / weightedCount));
        break;
      case SHORT:
        short scount = (short) (num.shortValue() / weightedCount);
        val = new Short(scount);
        break;
      default:
        val = new Double(num.doubleValue() / weightedCount);
        break;
    }
    return (V) val;
  }
}
