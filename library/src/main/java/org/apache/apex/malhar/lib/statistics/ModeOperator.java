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

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * An implementation of BaseOperator that calculates most frequent value occurring in incoming data. <br>
 * <p>
 * <b>Input Port(s) : </b><br>
 * <b>data : </b> Data values input port. <br>
 * <br>
 * <b>Output Port(s) : </b> <br>
 * <b>mode : </b>Mode value output port. <br>
 * <br>
 * <b>StateFull : Yes</b>, value are aggregated over application window. <br>
 * <b>Partitions : No</b>, no will yeild wrong results. <br>
 * <br>
 * @displayName Mode
 * @category Stats and Aggregations
 * @tags numeric, integer, count
 * @since 0.3.4
 */
@OperatorAnnotation(partitionable = false)
public class ModeOperator<V extends Comparable<?>> extends BaseOperator
{
  /**
   * Value/Count map.
   */
  HashMap<V, Integer>   values = new  HashMap<V, Integer>();
  /**
   * Input data port that takes comparable values.
   */
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
  {
    /**
     * Computes sum and count with each tuple
     */
    @Override
    public void process(V tuple)
    {
      if (values.containsKey(tuple)) {
        Integer count = values.remove(tuple);
        values.put(tuple, count + 1);
      } else {
        values.put(tuple, 1);
      }
    }
  };

  /**
   * Output port that emits most frequent value in incoming data.
   */
  public final transient DefaultOutputPort<V> mode = new DefaultOutputPort<V>();

  /**
   * Emit mode value.
   */
  @Override
  public void endWindow()
  {
    V modeValue = null;
    int max = 0;
    for (Map.Entry<V, Integer> entry : values.entrySet()) {
      if (entry.getValue() > max) {
        modeValue = entry.getKey();
        max = entry.getValue();
      }
    }
    if (mode != null) {
      mode.emit(modeValue);
    }
  }
}
