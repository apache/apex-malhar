/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.chart;

import com.datatorrent.api.Context.OperatorContext;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * This is the chart operator that plots the average (mean) value of Y for each window.  X will be based on the timestamp derived from the window id
 * @param <K> The type of the key
 * @author David Yan <davidyan@malhar-inc.com>
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
