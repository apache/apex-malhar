/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAGContext;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class TimeSeriesChartOperator<T> extends ChartOperator<Number, T>
{
  private long currentWindowId = 0;
  private long windowWidth;

  @Override
  public void setup(OperatorContext context)
  {
    windowWidth = context.getApplicationAttributes().attrValue(DAGContext.STRAM_WINDOW_SIZE_MILLIS, null);
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
  }

  @Override
  public Number getX()
  {
    return new Long(currentWindowId >>> 32 + windowWidth * (currentWindowId & 0xffffffff));
  }

}
