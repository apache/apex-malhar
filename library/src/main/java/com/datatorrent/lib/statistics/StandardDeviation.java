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

import java.util.ArrayList;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * This operator computes variance and standard deviation over incoming data. <br>
 * <br>
 * <b>Input Port(s) : </b><br>
 * <b>data : </b> Data values input port. <br>
 * <br>
 * <b>Output Port(s) : </b> <br>
 * <b>variance : </b>Variance value output port. <br>
 * <b>standardDeviatin : </b>Variance value output port. <br>
 * <br>
 * <b>StateFull : Yes</b>, value are aggregated over application window. <br>
 * <b>Partitions : No</b>, no will yield wrong results. <br>
 * <br>
 *
 * @since 0.3.4
 */
@OperatorAnnotation(partitionable = false)
public class StandardDeviation extends BaseOperator
{
  private ArrayList<Double> values = new ArrayList<Double>();
  
  /**
   * Input data port.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Number> data = new DefaultInputPort<Number>()
  {
    /**
     * Computes sum and count with each tuple
     */
    @Override
    public void process(Number tuple)
    {
      values.add(tuple.doubleValue());
    }
  };
  
  /**
   * Variance output port
   */
  @OutputPortFieldAnnotation(name = "variance", optional=true)
  public final transient DefaultOutputPort<Number> variance = new DefaultOutputPort<Number>();
  
  /**
   * Standard deviation output port
   */
  @OutputPortFieldAnnotation(name = "standardDeviation")
  public final transient DefaultOutputPort<Number> standardDeviation = new DefaultOutputPort<Number>();
  
  /**
   * End window.
   */
  @Override
  public void endWindow()
  {
    // no values.
    if (values.size() == 0) return;
    
    // get mean first.
    double mean = 0.0;
    for (Double value : values) {
      mean += value;
    }
    mean = mean/values.size();
    
    // get variance  
    double outVal = 0.0;
    for (Double value : values) {
      outVal += (value-mean)*(value-mean);
    }
    outVal = outVal / values.size();
    if (variance.isConnected()) {
      variance.emit(outVal);
    }
    
    // get standard deviation
    standardDeviation.emit(Math.sqrt(outVal));
    
    values = new ArrayList<Double>();
  }
}
