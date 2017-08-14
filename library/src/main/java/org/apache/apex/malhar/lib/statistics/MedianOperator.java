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

import java.util.ArrayList;
import java.util.Collections;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * An implementation of BaseOperator that computes median of incoming data. <br>
 * <p>
 * <b>Input Port(s) : </b><br>
 * <b>data : </b> Data values input port. <br>
 * <br>
 * <b>Output Port(s) : </b> <br>
 * <b>median : </b>Median output port. <br>
 * <br>
 * <b>StateFull : Yes</b>, value are aggregated over application window. <br>
 * <b>Partitions : No</b>, no will yield wrong results. <br>
 * <br>+
 * @displayName Median
 * @category Stats and Aggregations
 * @tags median operator, number
 * @since 0.3.4
 */
@OperatorAnnotation(partitionable = false)
public class MedianOperator extends BaseOperator
{
  private ArrayList<Double> values;

  /**
   * Input data port that takes a number.
   */
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
   * Output port that emits median of incoming data.
   */
  public final transient DefaultOutputPort<Number> median = new DefaultOutputPort<Number>();

  @Override
  public void beginWindow(long arg0)
  {
    values = new ArrayList<Double>();
  }

  @Override
  public void endWindow()
  {
    if (values.size() == 0) {
      return;
    }
    if (values.size() == 1) {
      median.emit(values.get(0));
      return;
    }

    // median value
    Collections.sort(values);
    int medianIndex = values.size() / 2;
    if (values.size() % 2 == 0) {
      Double value = values.get(medianIndex - 1);
      value = (value + values.get(medianIndex)) / 2;
      median.emit(value);
    } else {
      median.emit(values.get(medianIndex));
    }
  }

}
