/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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

import java.util.Iterator;
import java.util.TreeSet;

import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
  * This operator computes median of incoming data. <br>
 * <br>
 * <b>Input Port(s) : </b><br>
 * <b>data : </b> Data values input port. <br>
 * <br>
 * <b>Output Port(s) : </b> <br>
 * <b>median : </b>Median output port. <br>
 * <br>
 * <b>StateFull : Yes</b>, value are aggregated over application window. <br>
 * <b>Partitions : No</b>, no will yield wrong results. <br>
 * <br>+
*/
public class MedianOperator<V extends Number> extends BaseOperator
{
  private TreeSet<V> values;
  
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
      values.add(tuple);
    }
  };
  
  /**
   * Output port
   */
  @OutputPortFieldAnnotation(name = "median")
  public final transient DefaultOutputPort<V> median = new DefaultOutputPort<V>();
  
  @Override
  public void beginWindow(long arg0)
  {
    values = new TreeSet<V> ();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void endWindow()
  {
    if (values.size() == 0) return;
    Iterator<V> iter = values.iterator();
    if (values.size() == 1) {
      median.emit(iter.next());
    }
    
    // median value 
    V[] arr = (V[]) values.toArray();
    if (arr.length %2 == 0) {
      Number value = (Double) arr[arr.length/2];
      value =(Number) ((value.doubleValue() + arr[arr.length/2-1].doubleValue())/2);
      median.emit(((V)value));
    } else {
      median.emit(arr[arr.length/2]);
    }
  }

}
