/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * This is the base implementation of an enum charting operator.&nbsp;
 * This operator displays values on an x-axis and y-axis where the x-axis may be non-numeric and where the y-axis is numeric.&nbsp;
 * Subclasses must implement the method which derives the x-axis from tuples.
 * <p></p>
 * @displayName Abstract Enum Chart Operator
 * @category Charting
 * @tags output operator
 *
 * @param <K> The type of the key
 * @param <X> The type of values on the X-axis
 * @since 0.3.2
 */
public abstract class EnumChartOperator<K, X> extends XYChartOperator<K, X, Number>
{
  /**
   * The data map to store the data to be charted during a window
   */
  protected Map<K, Map<X, Number>> dataMap = new TreeMap<K, Map<X, Number>>();

  @Override
  public Type getChartType()
  {
    return Type.ENUM;
  }

  @Override
  public Map<X, Number> retrievePoints(K key)
  {
    return dataMap.get(key);
  }

  @Override
  public Collection<K> retrieveKeys()
  {
    return dataMap.keySet();
  }

  /**
   * Tells the chart operator how to get the X value to plot on the graph for this tuple
   * @param tuple
   * @return The X value to plot on the graph
   */
  public abstract X convertTupleToX(Object tuple);

  @Override
  public void processTuple(Object tuple)
  {
    K key = convertTupleToKey(tuple);
    X x = convertTupleToX(tuple);
    Number number = convertTupleToY(tuple);
    if (number != null) {
      Map<X, Number> map = dataMap.get(key);
      if (map == null) {
        map = new TreeMap<X, Number>();
        dataMap.put(key, map);
      }
      Number oldValue = map.get(x);
      if (yNumberType == NumberType.FLOAT) {
        double value = number.doubleValue();
        if (oldValue == null) {
          map.put(x, value);
        }
        else {
          map.put(x, oldValue.doubleValue() + value);
        }
      }
      else {
        double value = number.longValue();
        if (oldValue == null) {
          map.put(x, value);
        }
        else {
          map.put(x, oldValue.longValue() + value);
        }
      }
    }
  }

}
