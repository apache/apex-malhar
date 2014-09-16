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

import com.datatorrent.api.Context.OperatorContext;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * <p>
 * This is a chart operator that plots the average value for each window.
 * <p>
 * This is the chart operator that plots the average (mean) value of Y for each window.  X will be based on the timestamp derived from the window id
 *
 * @displayName Time Series Average Chart Operator
 * @category chart
 * @tags output
 *
 * @param <K> The type of the key
 * @since 0.3.2
 */
public class TimeSeriesAverageChartOperator<K> extends TimeSeriesChartOperator<K, Number>
{
  protected static class SumNumItems
  {
    double sum = 0.0;
    long numItems = 0;
  }

  /**
   *  The data map to store the data to be charted during a window
   */
  protected transient Map<K, SumNumItems> dataMap = new TreeMap<K, SumNumItems>();

  @Override
  public Type getChartType()
  {
    return Type.LINE;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    dataMap.clear();
  }

  @Override
  public Number getY(K key)
  {
    SumNumItems sni = dataMap.get(key);
    if (sni == null) {
      return null;
    }
    return (sni.numItems == 0) ? null : new Double(sni.sum / sni.numItems);
  }

  @Override
  public Collection<K> retrieveKeys()
  {
    return dataMap.keySet();
  }

  @Override
  public void processTuple(Object tuple)
  {
    K key = convertTupleToKey(tuple);
    Number number = convertTupleToY(tuple);
    if (number != null) {
      SumNumItems sni = dataMap.get(key);
      if (sni != null) {
        sni.sum += number.doubleValue();
        sni.numItems++;
      } else {
        sni = new SumNumItems();
        sni.sum = number.doubleValue();
        sni.numItems = 1;
        dataMap.put(key, sni);
      }
    }
  }

}
