/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.KeyValPair;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @param <K> The type for the key
 * @param <X> The type for the data points on the x-axis
 * @param <Y> The type for the data points on the y-axis
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class XYChartOperator<K, X, Y> extends ChartOperator
{
  public enum NumberType { LONG, DOUBLE };

  private String xAxisLabel;
  private String yAxisLabel;
  protected NumberType yNumberType = NumberType.LONG;

  public String getxAxisLabel()
  {
    return xAxisLabel;
  }

  public void setxAxisLabel(String xAxisLabel)
  {
    this.xAxisLabel = xAxisLabel;
  }

  public String getyAxisLabel()
  {
    return yAxisLabel;
  }

  public void setyAxisLabel(String yAxisLabel)
  {
    this.yAxisLabel = yAxisLabel;
  }

  @InputPortFieldAnnotation(name = "in1")
  public final transient DefaultInputPort<Object> in1 = new DefaultInputPort<Object>(this)
  {
    @Override
    public void process(Object tuple)
    {
      processTuple(tuple);
    }

  };
  @OutputPortFieldAnnotation(name = "chart")
  public final transient DefaultOutputPort<Map<K, Map<X, Y>>> chart = new DefaultOutputPort<Map<K, Map<X, Y>>>(this);

  @Override
  public void endWindow()
  {
    Map<K, Map<X, Y>> map = new TreeMap<K, Map<X, Y>>();
    for (K k: getKeys()) {
      K key = k;
      Map<X, Y> points = getPoints(key);
      if (!points.isEmpty()) {
        map.put(key, points);
      }
    }
    if (!map.isEmpty()) {
      chart.emit(map);
    }
  }

  public abstract Map<X, Y> getPoints(K key);

  public abstract Collection<K> getKeys();

  public abstract void processTuple(Object tuple);

  public void setyNumberType(NumberType numberType)
  {
    this.yNumberType = yNumberType;
  }

  public NumberType getyNumberType()
  {
    return yNumberType;
  }

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

  @SuppressWarnings("unchecked")
  public K convertTupleToKey(Object tuple)
  {
    if (tuple instanceof KeyValPair) {
      KeyValPair<?, ?> kvp = (KeyValPair<?, ?>)tuple;
      return (K)kvp.getKey();
    }
    return null; // default key is null
  }


  private static final Logger logger = LoggerFactory.getLogger(XYChartOperator.class);
}
