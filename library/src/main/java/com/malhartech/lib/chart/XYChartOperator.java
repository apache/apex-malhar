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
 * This is the base class for all chart operators that draw charts that have two axes
 * @param <K> The type for the key
 * @param <X> The type for the data points on the x-axis
 * @param <Y> The type for the data points on the y-axis
 * @author David Yan <davidyan@malhar-inc.com>
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
   * The input port of the chart operator.
   */
  @InputPortFieldAnnotation(name = "in1")
  public final transient DefaultInputPort<Object> in1 = new DefaultInputPort<Object>(this)
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
  @OutputPortFieldAnnotation(name = "chart")
  public final transient DefaultOutputPort<Map<K, Map<X, Y>>> chart = new DefaultOutputPort<Map<K, Map<X, Y>>>(this);

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


  private static final Logger logger = LoggerFactory.getLogger(XYChartOperator.class);
}
