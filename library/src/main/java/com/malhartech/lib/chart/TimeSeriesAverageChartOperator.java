/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.api.Context.OperatorContext;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class TimeSeriesAverageChartOperator extends TimeSeriesChartOperator<Number>
{
  protected double sum = 0.0;
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
    sum = 0.0;
    numData = 0;
  }

  @Override
  public Number getY()
  {
    return (numData == 0) ? null : new Double(sum / numData);
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
      sum += number.doubleValue();
      numData++;
    }
  }

}
