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
package com.datatorrent.lib.chart;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.KeyValPair;

/**
 * This is the base implementation for all chart operators that draw charts that have two axes.&nbsp;
 * Subclasses must implement the methods which retrieve data from and insert data into the chart.
 * <p></p>
 * @displayName Abstract XY Chart
 * @category Charting
 * @tags output operator
 *
 * @param <K> The type for the key
 * @param <X> The type for the data points on the x-axis
 * @param <Y> The type for the data points on the y-axis
 * @since 0.3.2
 */
public abstract class XYChartOperator<K, X, Y> extends ChartOperator
{
  /**
   * For distinguishing the number type of either the X-axis or the Y-axis values
   */
  public enum NumberType {
    /**
     * For integer type values
     */
    INTEGER,
    /**
     * For floating point type values
     */
    FLOAT,
  };

  private String xAxisLabel;
  private String yAxisLabel;
  /**
   * The value type of the Y-axis
   */
  protected NumberType yNumberType = NumberType.INTEGER;

  /**
   * Gets the label of the X-axis.
   * @return The label of the X-axis
   */
  public String getxAxisLabel()
  {
    return xAxisLabel;
  }

  /**
   * Sets the label of the X-axis.
   * @param xAxisLabel
   */
  public void setxAxisLabel(String xAxisLabel)
  {
    this.xAxisLabel = xAxisLabel;
  }

  /**
   * Gets the label of the Y-axis.
   * @return The label of the Y-axis
   */
  public String getyAxisLabel()
  {
    return yAxisLabel;
  }

  /**
   * @param yAxisLabel
   */
  public void setyAxisLabel(String yAxisLabel)
  {
    this.yAxisLabel = yAxisLabel;
  }

  /**
   * The input port on which tuples for plotting are received.
   */
  public final transient DefaultInputPort<Object> in1 = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      processTuple(tuple);
    }

  };

  /**
   * The output port of the chart operator.  The data of this will be shipped to the module that draws the chart.
   */
  public final transient ChartingOutputPort<Map<K, Map<X, Y>>> chart = new ChartingOutputPort<Map<K, Map<X, Y>>>();

  @Override
  public void endWindow()
  {
    Map<K, Map<X, Y>> map = new TreeMap<K, Map<X, Y>>();
    for (K k: retrieveKeys()) {
      K key = k;
      Map<X, Y> points = retrievePoints(key);
      if (!points.isEmpty()) {
        map.put(key, points);
      }
    }
    if (!map.isEmpty()) {
      chart.emit(map);
    }
  }

  /**
   * Gets all the points on the chart given the key within the window
   * @param key
   * @return Points on the chart
   */
  public abstract Map<X, Y> retrievePoints(K key);

  /**
   * Gets all the keys on the chart within the window
   * @return Keys
   */
  public abstract Collection<K> retrieveKeys();

  /**
   * Looks at the tuple and does what it needs to do in order to generate the chart at endWindow
   * @param tuple
   */
  public abstract void processTuple(Object tuple);

  /**
   * Sets the number type of Y-axis
   * @param numberType The number type, either INTEGER or FLOAT
   */
  public void setyNumberType(NumberType numberType)
  {
    this.yNumberType = numberType;
  }

  /**
   * Gets the number type of Y-axis
   * @return The number type
   */
  public NumberType getyNumberType()
  {
    return yNumberType;
  }

  /**
   * Tells the chart operator how to convert the tuple to its Y value
   * @param tuple
   * @return The number that contributes to the value on the Y-axis
   */
  public Number convertTupleToY(Object tuple)
  {
    if (tuple instanceof KeyValPair) {
      KeyValPair<?, ?> kvp = (KeyValPair<?, ?>)tuple;
      if (kvp.getValue() instanceof Number) {
        return (Number)kvp.getValue();
      }
      else {
        throw new RuntimeException("KeyValPair value is not a number. Consider overriding convertTupleToNumber");
      }
    }
    else if (tuple instanceof Number) {
      return (Number)tuple;
    }
    else {
      throw new RuntimeException("Tuple is not a number. Consider overriding convertTupleToNumber");
    }
  }

  /**
   * Tells the chart operator how to convert the tuple to its key
   * @param tuple
   * @return The key
   */
  @SuppressWarnings("unchecked")
  public K convertTupleToKey(Object tuple)
  {
    if (tuple instanceof KeyValPair) {
      KeyValPair<?, ?> kvp = (KeyValPair<?, ?>)tuple;
      return (K)kvp.getKey();
    }
    return null; // default key is null
  }
}
