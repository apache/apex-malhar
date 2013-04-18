/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.api.Context.OperatorContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class TimeSeriesAverageChartOperator extends TimeSeriesChartOperator<Number>
{
  protected static class SumNumItems
  {
    double sum = 0.0;
    long numItems = 0;
  }

  protected Map<Object, SumNumItems> dataMap = new HashMap<Object, SumNumItems>();

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
  public Number getY(Object key)
  {
    SumNumItems sni = dataMap.get(key);
    if (sni == null) {
      return null;
    }
    return (sni.numItems == 0) ? null : new Double(sni.sum / sni.numItems);
  }

  @Override
  public Collection<Object> getKeys()
  {
    return dataMap.keySet();
  }

  @Override
  public void processTuple(Object tuple)
  {
    Object key = convertTupleToKey(tuple);
    Number number = convertTupleToNumber(tuple);
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
