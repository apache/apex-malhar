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
 * @param <T1> The type for the data points on the x-axis
 * @param <T2> The type for the data points on the y-axis
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class XYChartOperator<T1, T2> extends ChartOperator
{
  private String xAxisLabel;
  private String yAxisLabel;

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
  public final transient DefaultOutputPort<Map<Object, KeyValPair<T1, T2>>> chart = new DefaultOutputPort<Map<Object, KeyValPair<T1, T2>>>(this);

  @Override
  public void endWindow()
  {
    HashMap<Object, KeyValPair<T1, T2>> map = new HashMap<Object, KeyValPair<T1, T2>>();
    for (Object key: getKeys()) {
      T1 x = getX(key);
      T2 y = getY(key);
      if (x != null && y != null) {
        map.put(key, new KeyValPair<T1, T2>(x, y));
      }
    }
    if (!map.isEmpty()) {
      chart.emit(map);
    }
  }

  public abstract T1 getX(Object key);

  public abstract T2 getY(Object key);

  public abstract Collection<Object> getKeys();

  public abstract void processTuple(Object tuple);

  public Number convertTupleToNumber(Object tuple)
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

  public Object convertTupleToKey(Object tuple)
  {
    if (tuple instanceof KeyValPair) {
      KeyValPair<?, ?> kvp = (KeyValPair<?, ?>)tuple;
      return kvp.getKey();
    }
    return ""; // default key is empty string
  }


  private static final Logger logger = LoggerFactory.getLogger(XYChartOperator.class);
}
