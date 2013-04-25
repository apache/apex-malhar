/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.util.HighLow;
import java.util.TreeMap;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class HistogramChartOperator<K> extends EnumChartOperator<K, HighLow>
{
  protected Number high;
  protected Number low;
  protected int numDivisions;
  protected NumberType xNumberType = NumberType.LONG;
  protected TreeMap<Number, HighLow> highLows = new TreeMap<Number, HighLow>();

  public Number getHigh()
  {
    return high;
  }

  public void setHigh(Number high)
  {
    this.high = high;
  }

  public Number getLow()
  {
    return low;
  }

  public void setLow(Number low)
  {
    this.low = low;
  }

  public int getNumDivisions()
  {
    return numDivisions;
  }

  public void setNumDivisions(int numDivisions)
  {
    this.numDivisions = numDivisions;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (numDivisions == 0) {
      throw new RuntimeException("setNumDivisions() must be called first");
    }
    if (xNumberType == NumberType.DOUBLE) {
      double increment = (high.doubleValue() - low.doubleValue()) / numDivisions;
      double current = low.doubleValue();
      if (increment <= 0) {
        throw new RuntimeException("increment will be <= 0 given high, low and numDivisions");
      }
      for (int i = 0; i < numDivisions; i++) {
        highLows.put(current, new HighLow(i == numDivisions - 1 ? high : current + increment, current));
        current += increment;
      }
    }
    else {
      long increment = (high.longValue() - low.longValue()) / numDivisions;
      long current = low.longValue();
      if (increment <= 0) {
        throw new RuntimeException("increment will be < 0 given high, low and numDivisions");
      }
      for (int i = 0; i < numDivisions; i++) {
        if (current + increment - 1 > high.longValue()) {
          throw new RuntimeException("Too many divisions for long (integer) x values");
        }
        highLows.put(current, new HighLow(i == numDivisions - 1 ? high : current + increment - 1, current));
        current += increment;
      }
    }
  }

  public abstract Number convertTupleToXSource(Object tuple);

  @Override
  public HighLow convertTupleToX(Object tuple)
  {
    Number number = convertTupleToXSource(tuple);
    if (xNumberType == NumberType.DOUBLE) {
      if (number.doubleValue() < low.doubleValue() || number.doubleValue() > high.doubleValue()) {
        // out of range
        return null;
      }
    }
    else {
      if (number.longValue() < low.longValue() || number.longValue() > high.longValue()) {
        return null;
      }
    }
    return highLows.higherEntry(number).getValue();
  }

}
