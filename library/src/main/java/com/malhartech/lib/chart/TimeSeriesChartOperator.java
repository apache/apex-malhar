/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAGContext;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @param <K>
 * @param <Y>
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
    windowWidth = context.getApplicationAttributes().attrValue(DAGContext.STRAM_WINDOW_SIZE_MILLIS, null);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    currentWindowId = windowId;
  }

  public Number getX(K key)
  {
    return new Long((currentWindowId >>> 32) * 1000 + windowWidth * (currentWindowId & 0xffffffffL));
  }

  public abstract Y getY(K key);

  @Override
  public Map<Number, Y> getPoints(K key) {
    Map<Number, Y> points = new TreeMap<Number, Y>();
    points.put(getX(key), getY(key));
    return points;
  }

}
