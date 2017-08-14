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
package org.apache.apex.malhar.lib.multiwindow;

import java.util.ArrayList;

import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Calculates simple moving average (SMA) of last N window. <br>
 * <p>
 * <b>StateFull : Yes</b>, operator store values  for n-1 th windows. <br>
 * <b>Partitions : No</b>, sum is not unified on output ports. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Expects KeyValPair where K is Object and V is Number.<br>
 * <b>doubleSMA</b>: Emits simple moving average of N window as Double.<br>
 * <b>floatSMA</b>: Emits simple moving average of N window as Float.<br>
 * <b>longSMA</b>: Emits simple moving average of N window as Long.<br>
 * <b>integerSMA</b>: Emits simple moving average of N window as Integer.<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>windowSize</b>: Number of windows to keep state on<br>
 * <br>
 * @displayName Simple Moving Average
 * @category Stats and Aggregations
 * @tags key value, numeric, average
 * @since 0.3.3
 */
@OperatorAnnotation(partitionable = false)
public class SimpleMovingAverage<K, V extends Number> extends
    AbstractSlidingWindowKeyVal<K, V, SimpleMovingAverageObject>
{
  /**
   * Output port to emit simple moving average (SMA) of last N window as Double.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Double>> doubleSMA = new DefaultOutputPort<KeyValPair<K, Double>>();
  /**
   * Output port to emit simple moving average (SMA) of last N window as Float.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Float>> floatSMA = new DefaultOutputPort<KeyValPair<K, Float>>();
  /**
   * Output port to emit simple moving average (SMA) of last N window as Long.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Long>> longSMA = new DefaultOutputPort<KeyValPair<K, Long>>();
  /**
   * Output port to emit simple moving average (SMA) of last N window as
   * Integer.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Integer>> integerSMA = new DefaultOutputPort<KeyValPair<K, Integer>>();

  /**
   * Create the list if key doesn't exist. Add value to buffer and increment
   * counter.
   *
   * @param tuple
   */
  @Override
  public void processDataTuple(KeyValPair<K, V> tuple)
  {
    K key = tuple.getKey();
    double val = tuple.getValue().doubleValue();
    ArrayList<SimpleMovingAverageObject> dataList = buffer.get(key);

    if (dataList == null) {
      dataList = new ArrayList<SimpleMovingAverageObject>(windowSize);
      for (int i = 0; i < windowSize; ++i) {
        dataList.add(new SimpleMovingAverageObject());
      }
    }

    dataList.get(currentstate).add(val); // add to previous value
    buffer.put(key, dataList);
  }

  /**
   * Calculate average and emit in appropriate port.
   *
   * @param key
   * @param obj
   */
  @Override
  public void emitTuple(K key, ArrayList<SimpleMovingAverageObject> obj)
  {
    double sum = 0;
    int count = 0;
    for (int i = 0; i < windowSize; i++) {
      SimpleMovingAverageObject d = obj.get(i);
      sum += d.getSum();
      count += d.getCount();
    }

    if (count == 0) { // Nothing to emit.
      return;
    }
    if (doubleSMA.isConnected()) {
      doubleSMA.emit(new KeyValPair<K, Double>(key, (sum / count)));
    }
    if (floatSMA.isConnected()) {
      floatSMA.emit(new KeyValPair<K, Float>(key, (float)(sum / count)));
    }
    if (longSMA.isConnected()) {
      longSMA.emit(new KeyValPair<K, Long>(key, (long)(sum / count)));
    }
    if (integerSMA.isConnected()) {
      integerSMA.emit(new KeyValPair<K, Integer>(key, (int)(sum / count)));
    }
  }
}
