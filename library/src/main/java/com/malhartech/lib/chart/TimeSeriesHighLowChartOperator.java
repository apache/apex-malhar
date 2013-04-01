/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.util.HighLow;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class TimeSeriesHighLowChartOperator extends TimeSeriesChartOperator<HighLow>
{
  protected double high = Double.MIN_VALUE;
  protected double low = Double.MAX_VALUE;
  protected long numData = 0;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    high = Double.MIN_VALUE;
    low = Double.MAX_VALUE;
    numData = 0;
  }

  @Override
  public HighLow getY()
  {
    return (numData > 0) ? new HighLow(high, low) : null;
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
    Number number = convertTupleToNumber(tuple);
    if (number != null) {
      double value = number.doubleValue();
      if (value > high) {
        high = value;
      }
      if (value < low) {
        low = value;
      }
      numData++;
    }
  }

}
