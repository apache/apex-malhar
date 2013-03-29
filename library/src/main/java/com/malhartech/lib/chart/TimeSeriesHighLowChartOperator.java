/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.lib.util.KeyValPair;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class TimeSeriesHighLowChartOperator extends TimeSeriesChartOperator<KeyValPair<Number,Number>>
{
  protected double high = Double.MIN_VALUE;
  protected double low = Double.MAX_VALUE;
  protected long numData = 0;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    high = Double.MIN_VALUE;
    low = Double.MAX_VALUE;
    numData = 0;
  }

  @Override
  public KeyValPair<Number,Number> getY()
  {
      return (numData > 0) ? new KeyValPair<Number,Number>(low, high) : null;
  }

  public Number convertTupleToNumber(Object tuple)
  {
    if (tuple instanceof Number) {
      return (Number)tuple;
    }
    else {
      throw new RuntimeException("Tuple is not a number");
    }
  }

  @Override
  public void processTuple(Object tuple)
  {
    double value = convertTupleToNumber(tuple).doubleValue();
    if (value > high) {
      high = value;
    }
    if (value < low) {
      low = value;
    }
    numData++;
  }

}
