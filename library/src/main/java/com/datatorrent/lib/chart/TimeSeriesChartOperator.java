/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.chart;

import com.datatorrent.api.DAGContext;
import com.datatorrent.api.Context.OperatorContext;

import java.util.Map;
import java.util.TreeMap;

/**
 * This is the base class for all chart operators that use X-axis as a time series
 * @param <K> The type of the key
 * @param <Y> The type of values on the Y-axis
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class TimeSeriesChartOperator<K, Y> extends XYChartOperator<K, Number, Y>
{
  private long currentWindowId = 0;
  private long windowWidth;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    windowWidth = context.attrValue(DAGContext.STREAMING_WINDOW_SIZE_MILLIS, null);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    currentWindowId = windowId;
  }

  /**
   * Gets the X point to plot on the graph
   * @param key
   * @return The value of X of the point
   */
  public Number getX(K key)
  {
    return new Long((currentWindowId >>> 32) * 1000 + windowWidth * (currentWindowId & 0xffffffffL));
  }

  /**
   * Gets the Y point to plot on the graph
   * @param key
   * @return The value of Y of the point
   */
  public abstract Y getY(K key);

  @Override
  public Map<Number, Y> retrievePoints(K key) {
    Map<Number, Y> points = new TreeMap<Number, Y>();
    points.put(getX(key), getY(key));
    return points;
  }

}
