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

import com.datatorrent.api.DAGContext;
import com.datatorrent.api.Context.OperatorContext;

import java.util.Map;
import java.util.TreeMap;

/**
 * This is the base class for all chart operators that use X-axis as a time series
 * <p></p>
 * @displayName Time Series Chart
 * @category chart
 * @tags output operator
 *
 * @param <K> The type of the key
 * @param <Y> The type of values on the Y-axis
 * @since 0.3.2
 */
public abstract class TimeSeriesChartOperator<K, Y> extends XYChartOperator<K, Number, Y>
{
  private long currentWindowId = 0;
  private long windowWidth;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    windowWidth = context.getValue(DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
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
