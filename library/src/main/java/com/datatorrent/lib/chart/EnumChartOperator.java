/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * This is the base class for all chart operators that plot charts with x-axis having enumerated values
 * @param <K> The type of the key
 * @param <X> The type of values on the X-axis
 * @author David Yan <davidyan@malhar-inc.com>
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
